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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.HashSet;
import java.util.Set;

import static io.pravega.connectors.flink.dynamic.table.PravegaOptions.*;

public class FlinkPravegaDynamicTableFactory implements
        DynamicTableSourceFactory,
        DynamicTableSinkFactory {

    public static final String IDENTIFIER = "pravega";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        ReadableConfig tableOptions = helper.getOptions();

        DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(
                DeserializationFormatFactory.class,
                FactoryUtil.FORMAT);

        // Validation
        helper.validate();
        PravegaOptions.validateTableSourceOptions(tableOptions);

        DataType producedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();

        return new FlinkPravegaDynamicTableSource(
                producedDataType,
                decodingFormat,
                PravegaOptions.getReaderGroupName(tableOptions),
                PravegaOptions.getPravegaConfig(tableOptions),
                PravegaOptions.resolveScanStreams(tableOptions),
                PravegaOptions.getReaderGroupRefreshTimeMillis(tableOptions),
                PravegaOptions.getCheckpointInitiateTimeoutMillis(tableOptions),
                PravegaOptions.getEventReadTimeoutMillis(tableOptions),
                PravegaOptions.getMaxOutstandingCheckpointRequest(tableOptions),
                PravegaOptions.getUid(tableOptions),
                PravegaOptions.isStreamingReader(tableOptions),
                PravegaOptions.isBoundedRead(tableOptions));
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        ReadableConfig tableOptions = helper.getOptions();

        EncodingFormat<SerializationSchema<RowData>> encodingFormat = helper.discoverEncodingFormat(
                SerializationFormatFactory.class,
                FactoryUtil.FORMAT);

        // Validation
        helper.validate();
        PravegaOptions.validateTableSinkOptions(tableOptions);

        TableSchema tableSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        return new FlinkPravegaDynamicTableSink(
                tableSchema,
                encodingFormat,
                PravegaOptions.getPravegaConfig(tableOptions),
                PravegaOptions.getSinkStream(tableOptions),
                PravegaOptions.getWriterMode(tableOptions),
                PravegaOptions.getTransactionLeaseRenewalIntervalMillis(tableOptions),
                PravegaOptions.isWatermarkPropagationEnabled(tableOptions),
                PravegaOptions.getRoutingKeyField(tableOptions));
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FactoryUtil.FORMAT);
        options.add(CONTROLLER_URI);
        options.add(SCOPE);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SECURITY_AUTH_TYPE);
        options.add(SECURITY_AUTH_TOKEN);
        options.add(SECURITY_VALIDATE_HOSTNAME);
        options.add(SECURITY_TRUST_STORE);
        options.add(SCAN_EXECUTION_TYPE);
        options.add(SCAN_STREAMS);
        options.add(SCAN_START_STREAMCUTS);
        options.add(SCAN_END_STREAMCUTS);
        options.add(SCAN_UID);
        options.add(SCAN_READER_GROUP_NAME);
        options.add(SCAN_READER_GROUP_MAX_OUTSTANDING_CHECKPOINT_REQUEST);
        options.add(SCAN_READER_GROUP_REFRESH_INTERVAL);
        options.add(SCAN_EVENT_READ_TIMEOUT_INTERVAL);
        options.add(SCAN_READER_GROUP_CHECKPOINT_INITIATE_TIMEOUT_INTERVAL);
        options.add(SINK_STREAM);
        options.add(SINK_SEMANTIC);
        options.add(SINK_TXN_LEASE_RENEWAL_INTERVAL);
        options.add(SINK_ENABLE_WATERMARK_PROPAGATION);
        options.add(SINK_ROUTINGKEY_FIELD_NAME);
        return options;
    }
}
