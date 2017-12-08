/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.flink;

import io.pravega.connectors.flink.util.StreamId;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.net.URI;
import java.util.Collections;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A table source to produce a streaming table from a Pravega stream.
 */
public class FlinkPravegaTableSource implements StreamTableSource<Row> {

    /** The Pravega controller endpoint. */
    private final URI controllerURI;

    /** The Pravega stream to use. */
    private final StreamId stream;

    /** The start time from when to read events from. */
    private final long startTime;

    /** Deserialization schema to use for Pravega stream events. */
    private final DeserializationSchema<Row> deserializationSchema;

    /** Type information describing the result type. */
    private final TypeInformation<Row> typeInfo;

    /**
     * Creates a Pravega {@link StreamTableSource}.
     *
     * <p>The {@code deserializationSchemaFactory} supplies a {@link DeserializationSchema}
     * based on the result type information.
     *
     * @param controllerURI                The pravega controller endpoint address.
     * @param stream                       The stream to read events from.
     * @param startTime                    The start time from when to read events from.
     * @param deserializationSchemaFactory The deserialization schema to use for stream events.
     * @param typeInfo                     The type information describing the result type.
     */
    public FlinkPravegaTableSource(
            final URI controllerURI,
            final StreamId stream,
            final long startTime,
            Function<TypeInformation<Row>, DeserializationSchema<Row>> deserializationSchemaFactory,
            TypeInformation<Row> typeInfo) {
        this.controllerURI = controllerURI;
        this.stream = stream;
        this.startTime = startTime;
        checkNotNull(deserializationSchemaFactory, "Deserialization schema factory");
        this.typeInfo = checkNotNull(typeInfo, "Type information");
        this.deserializationSchema = deserializationSchemaFactory.apply(typeInfo);
    }

    /**
     * NOTE: This method is for internal use only for defining a TableSource.
     *       Do not use it in Table API programs.
     */
    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment env) {
        FlinkPravegaReader<Row> reader = createFlinkPravegaReader();
        return env.addSource(reader);
    }

    @Override
    public TypeInformation<Row> getReturnType() {
        return typeInfo;
    }

    @Override
    public TableSchema getTableSchema() {
        return TableSchema.fromTypeInfo(typeInfo);
    }

    /**
     * Returns the low-level reader.
     */
    protected FlinkPravegaReader<Row> createFlinkPravegaReader() {
        return new FlinkPravegaReader<>(
                controllerURI,
                stream.getScope(), Collections.singleton(stream.getName()),
                startTime,
                deserializationSchema);
    }

    /**
     * Returns the deserialization schema.
     *
     * @return The deserialization schema
     */
    protected DeserializationSchema<Row> getDeserializationSchema() {
        return deserializationSchema;
    }

    @Override
    public String explainSource() {
        return "";
    }
}
