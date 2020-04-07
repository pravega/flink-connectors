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

import lombok.Getter;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An append-only table sink to emit a streaming table as a Pravega stream.
 */
public abstract class FlinkPravegaTableSink implements AppendStreamTableSink<Row>, BatchTableSink<Row> {

    /** A factory for the stream writer. */
    protected final Function<TableSinkConfiguration, FlinkPravegaWriter<Row>> writerFactory;

    /** A factory for output format. */
    protected final Function<TableSinkConfiguration, FlinkPravegaOutputFormat<Row>> outputFormatFactory;

    /** The effective table sink configuration */
    private TableSinkConfiguration tableSinkConfiguration;

    /**
     * Creates a Pravega {@link AppendStreamTableSink}.
     *
     * <p>Each row is written to a Pravega stream with a routing key based on the {@code routingKeyFieldName}.
     * The specified field must of type {@code STRING}.
     *
     * @param writerFactory                A factory for the stream writer.
     * @param outputFormatFactory          A factory for the output format.
     */
    protected FlinkPravegaTableSink(Function<TableSinkConfiguration, FlinkPravegaWriter<Row>> writerFactory,
                                    Function<TableSinkConfiguration, FlinkPravegaOutputFormat<Row>> outputFormatFactory) {
        this.writerFactory = Preconditions.checkNotNull(writerFactory, "writerFactory");
        this.outputFormatFactory = Preconditions.checkNotNull(outputFormatFactory, "outputFormatFactory");
    }

    /**
     * Creates a copy of the sink for configuration purposes.
     */
    protected abstract FlinkPravegaTableSink createCopy();

    /**
     * NOTE: This method is for internal use only for defining a TableSink.
     *       Do not use it in Table API programs.
     */
    @Override
    public void emitDataStream(DataStream<Row> dataStream) {
        throw new NotImplementedException("This method is deprecated and should not be called.");
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
        checkState(tableSinkConfiguration != null, "Table sink is not configured");
        FlinkPravegaWriter<Row> writer = writerFactory.apply(tableSinkConfiguration);
        return dataStream.addSink(writer);
    }

    @Override
    public void emitDataSet(DataSet<Row> dataSet) {
        checkState(tableSinkConfiguration != null, "Table sink is not configured");
        FlinkPravegaOutputFormat<Row> outputFormat = outputFormatFactory.apply(tableSinkConfiguration);
        dataSet.output(outputFormat);
    }

    @Override
    public TypeInformation<Row> getOutputType() {
        return new RowTypeInfo(getFieldTypes());
    }

    public String[] getFieldNames() {
        checkState(tableSinkConfiguration != null, "Table sink is not configured");
        return tableSinkConfiguration.fieldNames;
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        checkState(tableSinkConfiguration != null, "Table sink is not configured");
        return tableSinkConfiguration.fieldTypes;
    }

    @Override
    public FlinkPravegaTableSink configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        // called to configure the sink with a specific subset of fields
        checkNotNull(fieldNames, "fieldNames");
        checkNotNull(fieldTypes, "fieldTypes");
        Preconditions.checkArgument(fieldNames.length == fieldTypes.length,
                "Number of provided field names and types does not match.");

        FlinkPravegaTableSink copy = createCopy();
        copy.tableSinkConfiguration = new TableSinkConfiguration(fieldNames, fieldTypes);
        return copy;
    }

    /**
     * The table sink configuration which is provided by the table environment via {@code TableSink::configure}.
     */
    @Getter
    protected static class TableSinkConfiguration {
        // the set of projected fields and their types
        private String[] fieldNames;
        private TypeInformation[] fieldTypes;

        public TableSinkConfiguration(String[] fieldNames, TypeInformation[] fieldTypes) {
            this.fieldNames = Preconditions.checkNotNull(fieldNames, "fieldNames");
            this.fieldTypes = Preconditions.checkNotNull(fieldTypes, "fieldTypes");
        }
    }

    /**
     * An event router that extracts the routing key from a {@link Row} by field name.
     */
    static class RowBasedRouter implements PravegaEventRouter<Row> {

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

        int getKeyIndex() {
            return keyIndex;
        }
    }

    /**
     * An abstract {@link FlinkPravegaTableSink} builder.
     * @param <B> the builder type.
     */
    @Internal
    public abstract static class AbstractTableSinkBuilder<B extends AbstractTableSinkBuilder> extends AbstractStreamingWriterBuilder<Row, B> {

        private String routingKeyFieldName;

        /**
         * Sets the field name to use as a Pravega event routing key.
         *
         * Each row is written to a Pravega stream with a routing key based on the given field name.
         * The specified field must of type {@code STRING}.
         *
         * @param fieldName the field name.
         */
        public B withRoutingKeyField(String fieldName) {
            this.routingKeyFieldName = fieldName;
            return builder();
        }

        // region Internal

        /**
         * Gets a serialization schema based on the given output field names.
         * @param fieldNames the field names to emit.
         */
        protected abstract SerializationSchema<Row> getSerializationSchema(String[] fieldNames);

        /**
         * Creates the sink function based on the given table sink configuration and current builder state.
         *
         * @param configuration the table sink configuration, incl. projected fields
         */
        protected FlinkPravegaWriter<Row> createSinkFunction(TableSinkConfiguration configuration) {
            Preconditions.checkState(routingKeyFieldName != null, "The routing key field must be provided.");
            SerializationSchema<Row> serializationSchema = getSerializationSchema(configuration.getFieldNames());
            PravegaEventRouter<Row> eventRouter = new RowBasedRouter(routingKeyFieldName, configuration.getFieldNames(), configuration.getFieldTypes());
            return createSinkFunction(serializationSchema, eventRouter);
        }

        /**
         * Creates FlinkPravegaOutputFormat based on the given table sink configuration and current builder state.
         *
         * @param configuration the table sink configuration, incl. projected fields
         */
        protected FlinkPravegaOutputFormat<Row> createOutputFormat(TableSinkConfiguration configuration) {
            Preconditions.checkState(routingKeyFieldName != null, "The routing key field must be provided.");
            SerializationSchema<Row> serializationSchema = getSerializationSchema(configuration.getFieldNames());
            PravegaEventRouter<Row> eventRouter = new RowBasedRouter(routingKeyFieldName, configuration.getFieldNames(), configuration.getFieldTypes());
            return new FlinkPravegaOutputFormat<>(
                            getPravegaConfig().getClientConfig(),
                            resolveStream(),
                            serializationSchema,
                            eventRouter
                    );
        }

        // endregion
    }
}
