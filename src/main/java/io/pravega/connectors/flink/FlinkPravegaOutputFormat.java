/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.flink;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A Flink {@link OutputFormat} that can be added as a sink to write into Pravega. The current implementation does not
 * support transactional writes. The {@link FlinkPravegaOutputFormat} is used in {@link FlinkPravegaTableSink}
 * implementation to support writing to Pravega as part of batch {@link org.apache.flink.api.java.DataSet} operation.
 * See io.pravega.connectors.flink.FlinkTableITCase for more details on how to use it.
 */
@Slf4j
public class FlinkPravegaOutputFormat<T> extends RichOutputFormat<T> {

    private static final long serialVersionUID = 1L;

    // The name of Pravega stream to write into.
    private final String stream;

    // The name of Pravega scope where the stream belongs to.
    private final String scope;

    // The supplied event serializer.
    private final SerializationSchema<T> serializationSchema;

    // The factory used to create Pravega clients; closing this will also close all Pravega connections.
    private transient EventStreamClientFactory clientFactory;

    // The Pravega client config.
    private final ClientConfig clientConfig;

    // The router used to partition events within a stream.
    private final PravegaEventRouter<T> eventRouter;

    // Pravega event writer instance.
    private transient EventStreamWriter<T> pravegaWriter;

    // Error which will be detected asynchronously and reported to Flink.
    private final AtomicReference<Throwable> writeError;

    // Used to track confirmation from all writes to ensure guaranteed writes upon close.
    private final AtomicInteger pendingWritesCount;

    private transient ExecutorService executorService;

    /**
     * Creates a new Flink Pravega {@link OutputFormat} which can be added as a sink to a Flink batch job.
     *
     * @param clientConfig          The Pravega client configuration.
     * @param stream                The stream to write the events.
     * @param serializationSchema   The implementation to serialize events that will be written to pravega stream.
     * @param eventRouter           The event router to be used while writing the events.
     */
    protected FlinkPravegaOutputFormat(
            final ClientConfig clientConfig,
            final Stream stream,
            final SerializationSchema<T> serializationSchema,
            final PravegaEventRouter<T> eventRouter) {
        this.clientConfig = Preconditions.checkNotNull(clientConfig, "clientConfig");
        Preconditions.checkNotNull(stream, "stream");
        this.stream = stream.getStreamName();
        this.scope = stream.getScope();
        this.serializationSchema = Preconditions.checkNotNull(serializationSchema, "serializationSchema");
        this.eventRouter = Preconditions.checkNotNull(eventRouter, "eventRouter");
        this.writeError = new AtomicReference<>(null);
        this.pendingWritesCount = new AtomicInteger(0);
    }


    @Override
    public void configure(Configuration parameters) {
        //nothing
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        Serializer<T> eventSerializer = new FlinkPravegaWriter.FlinkSerializer<>(serializationSchema);
        EventWriterConfig writerConfig = EventWriterConfig.builder().build();
        clientFactory = createClientFactory(scope, clientConfig);
        pravegaWriter = clientFactory.createEventWriter(stream, eventSerializer, writerConfig);
        this.executorService = createExecutorService();
    }

    @Override
    public void writeRecord(T record) throws IOException {
        checkWriteError();
        this.pendingWritesCount.incrementAndGet();
        final CompletableFuture<Void> future = pravegaWriter.writeEvent(eventRouter.getRoutingKey(record), record);
        future.whenCompleteAsync(
                (result, e) -> {
                    if (e != null) {
                        log.warn("Detected a write failure: {}", e);

                        // We will record only the first error detected, since this will mostly likely help with
                        // finding the root cause. Storing all errors will not be feasible.
                        writeError.compareAndSet(null, e);
                    }
                    synchronized (this) {
                        pendingWritesCount.decrementAndGet();
                        this.notify();
                    }
                },
                executorService
        );
    }

    @Override
    public void close() throws IOException {

        Exception exception = null;

        try {
            flushAndVerify();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        if (clientFactory != null) {
            // it will close the pravegaWriter as well
            clientFactory.close();
        }

        if (executorService != null) {
            try {
                executorService.shutdown();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }
        }

        if (exception != null) {
            throw new IOException("exception occurred while trying to close the writer", exception);
        }

    }

    private void flushAndVerify() throws IOException {
        pravegaWriter.flush();

        // Wait until all errors, if any, have been recorded.
        synchronized (this) {
            while (this.pendingWritesCount.get() > 0) {
                try {
                    this.wait();
                } catch (InterruptedException e) {
                    throw new IOException("received interrupted exception while waiting for the writes to complete", e);
                }
            }
        }

        // Verify that no events have been lost so far.
        checkWriteError();
    }

    @VisibleForTesting
    protected void checkWriteError() throws IOException {
        Throwable error = this.writeError.getAndSet(null);
        if (error != null) {
            throw new IOException("Write failure", error);
        }
    }

    @VisibleForTesting
    protected EventStreamClientFactory createClientFactory(String scopeName, ClientConfig clientConfig) {
        return EventStreamClientFactory.withScope(scopeName, clientConfig);
    }

    @VisibleForTesting
    protected ExecutorService createExecutorService() {
        return Executors.newSingleThreadExecutor();
    }

    @VisibleForTesting
    protected SerializationSchema<T> getSerializationSchema() {
        return serializationSchema;
    }

    @VisibleForTesting
    protected String getStream() {
        return stream;
    }

    @VisibleForTesting
    protected String getScope() {
        return scope;
    }

    @VisibleForTesting
    protected boolean isErrorOccurred() {
        return writeError.get() != null;
    }

    @VisibleForTesting
    protected AtomicInteger getPendingWritesCount() {
        return pendingWritesCount;
    }

    @VisibleForTesting
    protected PravegaEventRouter<T> getEventRouter() {
        return eventRouter;
    }

    public static <T> FlinkPravegaOutputFormat.Builder<T> builder() {
        return new Builder<>();
    }

    public static class Builder<T> extends AbstractWriterBuilder<Builder<T>> {

        private SerializationSchema<T> serializationSchema;

        private PravegaEventRouter<T> eventRouter;

        /**
         * Sets the serialization schema.
         *
         * @param serializationSchema The serialization schema
         */
        public Builder<T> withSerializationSchema(SerializationSchema<T> serializationSchema) {
            this.serializationSchema = serializationSchema;
            return builder();
        }

        /**
         * Sets the event router.
         *
         * @param eventRouter the event router which produces a key per event.
         */
        public Builder<T> withEventRouter(PravegaEventRouter<T> eventRouter) {
            this.eventRouter = eventRouter;
            return builder();
        }

        @Override
        protected Builder<T> builder() {
            return this;
        }

        /**
         * Builds the {@link FlinkPravegaOutputFormat}.
         */
        public FlinkPravegaOutputFormat<T> build() {
            Preconditions.checkNotNull(serializationSchema, "serializationSchema");
            Preconditions.checkNotNull(eventRouter, "eventRouter");
            return new FlinkPravegaOutputFormat<>(
                            getPravegaConfig().getClientConfig(),
                            resolveStream(),
                            serializationSchema,
                            eventRouter
                    );
        }
    }

}
