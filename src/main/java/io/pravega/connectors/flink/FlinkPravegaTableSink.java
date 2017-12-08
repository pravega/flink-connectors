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
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.net.URI;
import java.util.Arrays;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An append-only table sink to emit a streaming table as a Pravaga stream.
 */
public class FlinkPravegaTableSink implements AppendStreamTableSink<Row> {

    /** The Pravega controller endpoint. */
    protected final URI controllerURI;

    /** The Pravega stream to use. */
    protected final StreamId stream;

    protected SerializationSchema<Row> serializationSchema;
    protected PravegaEventRouter<Row> eventRouter;
    protected String[] fieldNames;
    protected TypeInformation[] fieldTypes;

    /** Serialization schema to use for Pravega stream events. */
    private final Function<String[], SerializationSchema<Row>> serializationSchemaFactory;

    private final String routingKeyFieldName;

    /**
     * Creates a Pravega {@link AppendStreamTableSink}.
     *
     * <p>The {@code serializationSchemaFactory} supplies a {@link SerializationSchema}
     * based on the output field names.
     *
     * <p>Each row is written to a Pravega stream with a routing key based on the {@code routingKeyFieldName}.
     * The specified field must of type {@code STRING}.
     *
     * @param controllerURI                The pravega controller endpoint address.
     * @param stream                       The stream to write events to.
     * @param serializationSchemaFactory   A factory for the serialization schema to use for stream events.
     * @param routingKeyFieldName          The field name to use as a Pravega event routing key.
     */
    public FlinkPravegaTableSink(
            URI controllerURI,
            StreamId stream,
            Function<String[], SerializationSchema<Row>> serializationSchemaFactory,
            String routingKeyFieldName) {
        this.controllerURI = controllerURI;
        this.stream = stream;
        this.serializationSchemaFactory = serializationSchemaFactory;
        this.routingKeyFieldName = routingKeyFieldName;
    }

    /**
     * Creates a copy of the sink for configuration purposes.
     */
    protected FlinkPravegaTableSink createCopy() {
        return new FlinkPravegaTableSink(controllerURI, stream, serializationSchemaFactory, routingKeyFieldName);
    }

    /**
     * Returns the low-level writer.
     */
    protected FlinkPravegaWriter<Row> createFlinkPravegaWriter() {
        return new FlinkPravegaWriter<>(controllerURI, stream.getScope(), stream.getName(), serializationSchema, eventRouter);
    }

    /**
     * NOTE: This method is for internal use only for defining a TableSink.
     *       Do not use it in Table API programs.
     */
    @Override
    public void emitDataStream(DataStream<Row> dataStream) {
        checkState(fieldNames != null, "Table sink is not configured");
        checkState(fieldTypes != null, "Table sink is not configured");
        checkState(serializationSchema != null, "Table sink is not configured");
        checkState(eventRouter != null, "Table sink is not configured");

        FlinkPravegaWriter<Row> writer = createFlinkPravegaWriter();
        dataStream.addSink(writer);
    }

    @Override
    public TypeInformation<Row> getOutputType() {
        return new RowTypeInfo(getFieldTypes());
    }

    public String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return fieldTypes;
    }

    @Override
    public FlinkPravegaTableSink configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {

        // called to configure the sink with a specific subset of fields

        FlinkPravegaTableSink copy = createCopy();
        copy.fieldNames = checkNotNull(fieldNames, "fieldNames");
        copy.fieldTypes = checkNotNull(fieldTypes, "fieldTypes");
        Preconditions.checkArgument(fieldNames.length == fieldTypes.length,
                "Number of provided field names and types does not match.");

        copy.serializationSchema = serializationSchemaFactory.apply(fieldNames);
        copy.eventRouter = new RowBasedRouter(routingKeyFieldName, fieldNames, fieldTypes);

        return copy;
    }

    /**
     * An event router that extracts the routing key from a {@link Row} by field name.
     */
    private static class RowBasedRouter implements PravegaEventRouter<Row> {

        private final int keyIndex;

        public RowBasedRouter(String keyFieldName, String[] fieldNames, TypeInformation<?>[] fieldTypes) {
            checkArgument(fieldNames.length == fieldTypes.length,
                    "Number of provided field names and types does not match.");
            int keyIndex = Arrays.asList(fieldNames).indexOf(keyFieldName);
            checkArgument(keyIndex >= 0,
                    "Key field '" + keyFieldName + "' not found");
            checkArgument(Types.STRING.equals(fieldTypes[keyIndex]),
                    "Key field must be of type 'STRING'");
            this.keyIndex = keyIndex;
        }

        @Override
        public String getRoutingKey(Row event) {
            return (String) event.getField(keyIndex);
        }
    }
}
