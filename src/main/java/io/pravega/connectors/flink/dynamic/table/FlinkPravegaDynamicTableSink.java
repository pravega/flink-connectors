/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.connectors.flink.dynamic.table;

import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.PravegaEventRouter;
import io.pravega.connectors.flink.PravegaWriterMode;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;

public class FlinkPravegaDynamicTableSink implements DynamicTableSink {

    // Consumed data type of the table
    private final TableSchema tableSchema;

    // Sink format for encoding records to Pravega
    private final EncodingFormat<SerializationSchema<RowData>> encodingFormat;

    // Pravega connection configuration
    private final PravegaConfig pravegaConfig;

    // Pravega sink stream
    private final Stream stream;

    // Pravega writer mode
    private final PravegaWriterMode writerMode;

    // Transaction lease renewal period, valid for exactly-once semantic
    private final long txnLeaseRenewalIntervalMillis;

    // Flag to enable watermark propagation from Flink table to Pravega stream
    private final boolean enableWatermarkPropagation;

    // Pravega routing key field name
    @Nullable
    private final String routingKeyFieldName;

    /**
     * Creates a Pravega {@link DynamicTableSink}.
     *
     * <p>Each row is written to a Pravega stream with a routing key based on the {@code routingKeyFieldName}.
     * The specified field must of type {@code STRING}.
     *
     * @param tableSchema                   The table schema
     * @param encodingFormat                sink format for encoding records to Pravega
     * @param pravegaConfig                 Pravega connection configuration
     * @param stream                        Pravega sink stream
     * @param writerMode                    Pravega writer mode
     * @param txnLeaseRenewalIntervalMillis transaction lease renewal period
     * @param enableWatermarkPropagation    enable watermark propagation from Flink table to Pravega stream
     * @param routingKeyFieldName           field name as Pravega routing key
     */
    public FlinkPravegaDynamicTableSink(TableSchema tableSchema,
                                        EncodingFormat<SerializationSchema<RowData>> encodingFormat,
                                        PravegaConfig pravegaConfig,
                                        Stream stream,
                                        PravegaWriterMode writerMode,
                                        long txnLeaseRenewalIntervalMillis,
                                        boolean enableWatermarkPropagation,
                                        @Nullable String routingKeyFieldName) {
        this.tableSchema = Preconditions.checkNotNull(tableSchema, "Table schema must not be null.");
        this.encodingFormat = Preconditions.checkNotNull(encodingFormat, "Encoding format must not be null.");
        this.pravegaConfig = Preconditions.checkNotNull(pravegaConfig, "Pravega config must not be null.");
        this.stream = Preconditions.checkNotNull(stream, "Stream must not be null.");
        this.writerMode = Preconditions.checkNotNull(writerMode, "Writer mode must not be null.");
        this.txnLeaseRenewalIntervalMillis = txnLeaseRenewalIntervalMillis;
        this.enableWatermarkPropagation = enableWatermarkPropagation;
        this.routingKeyFieldName = routingKeyFieldName;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return this.encodingFormat.getChangelogMode();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        FlinkPravegaWriter.Builder<RowData> writerBuilder = FlinkPravegaWriter.<RowData>builder()
                .withPravegaConfig(pravegaConfig)
                .withSerializationSchema(encodingFormat.createRuntimeEncoder(context, this.tableSchema.toPhysicalRowDataType()))
                .forStream(stream)
                .withWriterMode(writerMode)
                .enableWatermark(enableWatermarkPropagation)
                .withTxnLeaseRenewalPeriod(Time.milliseconds(txnLeaseRenewalIntervalMillis));

        if (routingKeyFieldName != null) {
            writerBuilder.withEventRouter(new RowDataBasedRouter(routingKeyFieldName, tableSchema));
        }

        return SinkFunctionProvider.of(writerBuilder.build());
    }

    @Override
    public DynamicTableSink copy() {
        return new FlinkPravegaDynamicTableSink(
                this.tableSchema,
                this.encodingFormat,
                this.pravegaConfig,
                this.stream,
                this.writerMode,
                this.txnLeaseRenewalIntervalMillis,
                this.enableWatermarkPropagation,
                this.routingKeyFieldName);
    }

    @Override
    public String asSummaryString() {
        return "Pravega";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final FlinkPravegaDynamicTableSink that = (FlinkPravegaDynamicTableSink) o;
        return txnLeaseRenewalIntervalMillis == that.txnLeaseRenewalIntervalMillis &&
                enableWatermarkPropagation == that.enableWatermarkPropagation &&
                tableSchema.equals(that.tableSchema) &&
                encodingFormat.equals(that.encodingFormat) &&
                pravegaConfig.equals(that.pravegaConfig) &&
                stream.equals(that.stream) &&
                writerMode == that.writerMode &&
                Objects.equals(routingKeyFieldName, that.routingKeyFieldName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                tableSchema,
                encodingFormat,
                pravegaConfig,
                stream,
                writerMode,
                txnLeaseRenewalIntervalMillis,
                enableWatermarkPropagation,
                routingKeyFieldName);
    }

    /**
     * An event router that extracts the routing key from a {@link RowData} by field name.
     */
    public static class RowDataBasedRouter implements PravegaEventRouter<RowData> {

        private final int keyIndex;

        public RowDataBasedRouter(String routingKeyFieldName, TableSchema tableSchema) {
            String[] fieldNames = tableSchema.getFieldNames();
            int keyIndex = Arrays.asList(fieldNames).indexOf(routingKeyFieldName);

            checkArgument(keyIndex >= 0,
                    "Key field '" + routingKeyFieldName + "' not found");

            DataType[] fieldTypes = tableSchema.getFieldDataTypes();
            LogicalTypeRoot logicalTypeRoot = fieldTypes[keyIndex].getLogicalType().getTypeRoot();

            checkArgument(LogicalTypeRoot.CHAR == logicalTypeRoot || LogicalTypeRoot.VARCHAR == logicalTypeRoot,
                    "Key field must be of string type");

            this.keyIndex = keyIndex;
        }

        @Override
        public String getRoutingKey(RowData event) {
            return event.getString(keyIndex).toString();
        }
    }
}
