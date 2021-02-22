/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.connectors.flink.util;

import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.connectors.flink.EventTimeOrderingOperator;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.serialization.WrappingSerializer;
import io.pravega.shared.security.auth.Credentials;
import lombok.SneakyThrows;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.util.Preconditions;

import java.nio.ByteBuffer;
import java.util.Objects;

public class FlinkPravegaUtils {
    private static final String AUTH_PARAM_LOAD_DYNAMIC = "pravega.client.auth.loadDynamic";
    private static final String AUTH_PARAM_LOAD_DYNAMIC_ENV = "pravega_client_auth_loadDynamic";

    private FlinkPravegaUtils() {
    }

    /**
     * Writes a stream of elements to a Pravega stream with event time ordering.
     * <p>
     * This method returns a sink that automatically reorders events by their event timestamp.  The ordering
     * is preserved by Pravega using the associated routing key.
     *
     * @param stream      the stream to read.
     * @param writer      the Pravega writer to use.
     * @param parallelism the degree of parallelism for the writer.
     * @param <T>         The type of the event.
     * @return a sink.
     */
    public static <T> DataStreamSink<T> writeToPravegaInEventTimeOrder(DataStream<T> stream, FlinkPravegaWriter<T> writer, int parallelism) {
        // a keyed stream is used to ensure that all elements for a given key are forwarded to the same writer instance.
        // the parallelism must match between the ordering operator and the sink operator to ensure that
        // a forwarding strategy (as opposed to a rebalancing strategy) is used by Flink between the two operators.
        Preconditions.checkNotNull(writer.getEventRouter(), "Event router should not be null");
        return stream
                .keyBy(new PravegaEventRouterKeySelector<>(writer.getEventRouter()))
                .transform("reorder", stream.getType(), new EventTimeOrderingOperator<>()).setParallelism(parallelism).forward()
                .addSink(writer).setParallelism(parallelism);
    }

    /**
     * Utility method that derives the reader name from taskName, index and parallelism.
     *
     * @param taskName the original task name
     * @param index the index of the subtask
     * @param total the total parallelism of the subtask
     * @return the generated default reader name.
     */
    public static String getReaderName(final String taskName, final int index, final int total) {
        String shortTaskName = "";
        if (taskName.length() >= 200) {
            shortTaskName = taskName.substring(0, 200);
        }
        String readerName = "flink-task-" + shortTaskName + "-" + index + "-" + total;
        readerName = StringUtils.removePattern(readerName, "[^\\p{Alnum}\\.\\-]");
        return readerName;
    }

    /**
     * Generates a random reader group name.
     *
     * @return generated reader group name.
     */
    public static String generateRandomReaderGroupName() {
        return "flink" + RandomStringUtils.randomAlphanumeric(20).toLowerCase();
    }

    /**
     * Creates a Pravga {@link EventStreamReader}.
     *
     * @param readerId The id of the Pravega reader.
     * @param readerGroupName The reader group name.
     * @param deserializationSchema The implementation to deserialize events from pravega streams.
     * @param readerConfig The reader configuration.
     * @param eventStreamClientFactory The eventStreamClientFactory used to create the EventStreamReader
     * @param <T> The type of the event.
     * @return the create Pravega reader.
     */
    public static <T> EventStreamReader<T> createPravegaReader(
            String readerId,
            String readerGroupName,
            DeserializationSchema<T> deserializationSchema,
            ReaderConfig readerConfig,
            EventStreamClientFactory eventStreamClientFactory) {

        // create the adapter between Pravega's serializers and Flink's serializers
        @SuppressWarnings("unchecked")
        final Serializer<T> deserializer = deserializationSchema instanceof WrappingSerializer
                ? ((WrappingSerializer<T>) deserializationSchema).getWrappedSerializer()
                : new FlinkDeserializer<>(deserializationSchema);

        return eventStreamClientFactory.createReader(readerId, readerGroupName, deserializer, readerConfig);
    }

    /**
     * A Pravega {@link Serializer} that wraps around a Flink {@link DeserializationSchema}.
     *
     * @param <T> The type of the event.
     */
    public static final class FlinkDeserializer<T> implements Serializer<T> {

        private final DeserializationSchema<T> deserializationSchema;

        public FlinkDeserializer(DeserializationSchema<T> deserializationSchema) {
            this.deserializationSchema = deserializationSchema;
        }

        @Override
        public ByteBuffer serialize(T value) {
            throw new IllegalStateException("serialize() called within a deserializer");
        }

        @Override
        @SneakyThrows
        public T deserialize(ByteBuffer buffer) {
            byte[] array;
            if (buffer.hasArray() && buffer.arrayOffset() == 0 && buffer.position() == 0 && buffer.limit() == buffer.capacity()) {
                array = buffer.array();
            } else {
                array = new byte[buffer.remaining()];
                buffer.get(array);
            }

            return deserializationSchema.deserialize(array);
        }
    }

    public static final class SimpleCredentials implements Credentials {
        private final String authType;
        private final String authToken;

        public SimpleCredentials(String authType, String authToken) {
            this.authType = authType;
            this.authToken = authToken;
        }

        @Override
        public String getAuthenticationType() {
            return authType;
        }

        @Override
        public String getAuthenticationToken() {
            return authToken;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final SimpleCredentials that = (SimpleCredentials) o;
            return Objects.equals(authType, that.authType) &&
                    Objects.equals(authToken, that.authToken);
        }

        @Override
        public int hashCode() {
            return Objects.hash(authType, authToken);
        }
    }

    public static boolean isCredentialsLoadDynamic() {
        return System.getProperties().contains(AUTH_PARAM_LOAD_DYNAMIC) && Boolean.parseBoolean(System.getProperty(AUTH_PARAM_LOAD_DYNAMIC)) ||
                System.getenv().containsKey(AUTH_PARAM_LOAD_DYNAMIC_ENV) && Boolean.parseBoolean(System.getenv(AUTH_PARAM_LOAD_DYNAMIC_ENV));
    }
}
