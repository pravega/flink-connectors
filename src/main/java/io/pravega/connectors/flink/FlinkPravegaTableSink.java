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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An append-only table sink to emit a streaming table as a Pravega stream.
 *
 * @deprecated Please use the new Table API {@link io.pravega.connectors.flink.dynamic.table.FlinkPravegaDynamicTableSink}
 */
@Deprecated
public class FlinkPravegaTableSink implements AppendStreamTableSink<Row>, BatchTableSink<Row> {

    /** A factory for the stream writer. */
    protected final Function<TableSchema, FlinkPravegaWriter<Row>> writerFactory;

    /** A factory for output format. */
    protected final Function<TableSchema, FlinkPravegaOutputFormat<Row>> outputFormatFactory;

    /** The schema of the table. */
    protected TableSchema schema;

    /**
     * Creates a Pravega {@link AppendStreamTableSink}.
     *
     * <p>Each row is written to a Pravega stream with a routing key based on the {@code routingKeyFieldName}.
     * The specified field must of type {@code STRING}.
     *
     * @param writerFactory                A factory for the stream writer.
     * @param outputFormatFactory          A factory for the output format.
     * @param schema                       The table schema of the sink.
     */
    protected FlinkPravegaTableSink(Function<TableSchema, FlinkPravegaWriter<Row>> writerFactory,
                                    Function<TableSchema, FlinkPravegaOutputFormat<Row>> outputFormatFactory,
                                    TableSchema schema) {
        this.writerFactory = Preconditions.checkNotNull(writerFactory, "writerFactory");
        this.outputFormatFactory = Preconditions.checkNotNull(outputFormatFactory, "outputFormatFactory");
        this.schema = TableSchemaUtils.checkOnlyPhysicalColumns(schema);
    }

    /**
     * Creates a copy of the sink for configuration purposes.
     */
    private FlinkPravegaTableSink createCopy() {
        return new FlinkPravegaTableSink(writerFactory, outputFormatFactory, schema);
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
        checkState(schema != null, "Table sink is not configured");
        FlinkPravegaWriter<Row> writer = writerFactory.apply(schema);
        return dataStream.addSink(writer)
                .setParallelism(dataStream.getParallelism())
                .name(TableConnectorUtils.generateRuntimeName(this.getClass(), getFieldNames()));
    }

    @Override
    public DataSink<?> consumeDataSet(DataSet<Row> dataSet) {
        checkState(schema != null, "Table sink is not configured");
        FlinkPravegaOutputFormat<Row> outputFormat = outputFormatFactory.apply(schema);
        return dataSet.output(outputFormat);
    }

    @Override
    public DataType getConsumedDataType() {
        return schema.toRowDataType();
    }

    public TableSchema getTableSchema() {
        return schema;
    }

    @Override
    public FlinkPravegaTableSink configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        // called to configure the sink with a specific subset of fields
        checkNotNull(fieldNames, "fieldNames");
        checkNotNull(fieldTypes, "fieldTypes");
        Preconditions.checkArgument(fieldNames.length == fieldTypes.length,
                "Number of provided field names and types does not match.");

        FlinkPravegaTableSink copy = createCopy();
        DataType[] dataTypes = Arrays.stream(fieldTypes)
                .map(TypeConversions::fromLegacyInfoToDataType)
                .toArray(DataType[]::new);
        copy.schema = TableSchema.builder().fields(fieldNames, dataTypes).build();
        return copy;
    }

    /**
     * An event router that extracts the routing key from a {@link Row} by field name.
     */
    public static class RowBasedRouter implements PravegaEventRouter<Row> {

        private final int keyIndex;

        public RowBasedRouter(String keyFieldName, String[] fieldNames, DataType[] fieldTypes) {
            checkArgument(fieldNames.length == fieldTypes.length,
                    "Number of provided field names and types does not match.");
            int keyIndex = Arrays.asList(fieldNames).indexOf(keyFieldName);
            checkArgument(keyIndex >= 0,
                    "Key field '" + keyFieldName + "' not found");
            checkArgument(DataTypes.STRING().equals(fieldTypes[keyIndex]),
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
}
