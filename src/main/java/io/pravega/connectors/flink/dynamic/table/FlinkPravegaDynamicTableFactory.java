/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.connectors.flink.dynamic.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
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

import java.util.HashSet;
import java.util.Set;

import static io.pravega.connectors.flink.dynamic.table.PravegaOptions.CONTROLLER_URI;
import static io.pravega.connectors.flink.dynamic.table.PravegaOptions.SCAN_END_STREAMCUTS;
import static io.pravega.connectors.flink.dynamic.table.PravegaOptions.SCAN_EVENT_READ_TIMEOUT_INTERVAL;
import static io.pravega.connectors.flink.dynamic.table.PravegaOptions.SCAN_EXECUTION_TYPE;
import static io.pravega.connectors.flink.dynamic.table.PravegaOptions.SCAN_READER_GROUP_CHECKPOINT_INITIATE_TIMEOUT_INTERVAL;
import static io.pravega.connectors.flink.dynamic.table.PravegaOptions.SCAN_READER_GROUP_MAX_OUTSTANDING_CHECKPOINT_REQUEST;
import static io.pravega.connectors.flink.dynamic.table.PravegaOptions.SCAN_READER_GROUP_NAME;
import static io.pravega.connectors.flink.dynamic.table.PravegaOptions.SCAN_READER_GROUP_REFRESH_INTERVAL;
import static io.pravega.connectors.flink.dynamic.table.PravegaOptions.SCAN_START_STREAMCUTS;
import static io.pravega.connectors.flink.dynamic.table.PravegaOptions.SCAN_STREAMS;
import static io.pravega.connectors.flink.dynamic.table.PravegaOptions.SCAN_UID;
import static io.pravega.connectors.flink.dynamic.table.PravegaOptions.SCOPE;
import static io.pravega.connectors.flink.dynamic.table.PravegaOptions.SECURITY_AUTH_TOKEN;
import static io.pravega.connectors.flink.dynamic.table.PravegaOptions.SECURITY_AUTH_TYPE;
import static io.pravega.connectors.flink.dynamic.table.PravegaOptions.SECURITY_TRUST_STORE;
import static io.pravega.connectors.flink.dynamic.table.PravegaOptions.SECURITY_VALIDATE_HOSTNAME;
import static io.pravega.connectors.flink.dynamic.table.PravegaOptions.SINK_ENABLE_WATERMARK_PROPAGATION;
import static io.pravega.connectors.flink.dynamic.table.PravegaOptions.SINK_ROUTINGKEY_FIELD_NAME;
import static io.pravega.connectors.flink.dynamic.table.PravegaOptions.SINK_SEMANTIC;
import static io.pravega.connectors.flink.dynamic.table.PravegaOptions.SINK_STREAM;
import static io.pravega.connectors.flink.dynamic.table.PravegaOptions.SINK_TXN_LEASE_RENEWAL_INTERVAL;

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
        PravegaOptionsUtil.validateTableSourceOptions(tableOptions);

        DataType producedDataType = context.getPhysicalRowDataType();

        return new FlinkPravegaDynamicTableSource(
                producedDataType,
                decodingFormat,
                PravegaOptionsUtil.getReaderGroupName(tableOptions),
                PravegaOptionsUtil.getPravegaConfig(tableOptions),
                PravegaOptionsUtil.resolveScanStreams(tableOptions),
                PravegaOptionsUtil.getReaderGroupRefreshTimeMillis(tableOptions),
                PravegaOptionsUtil.getCheckpointInitiateTimeoutMillis(tableOptions),
                PravegaOptionsUtil.getEventReadTimeoutMillis(tableOptions),
                PravegaOptionsUtil.getMaxOutstandingCheckpointRequest(tableOptions),
                PravegaOptionsUtil.getUid(tableOptions),
                PravegaOptionsUtil.isStreamingReader(tableOptions),
                PravegaOptionsUtil.isBoundedRead(tableOptions));
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
        PravegaOptionsUtil.validateTableSinkOptions(tableOptions);

        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        final DataType physicalDatatype = context.getPhysicalRowDataType();

        return new FlinkPravegaDynamicTableSink(
                physicalDatatype,
                resolvedSchema,
                encodingFormat,
                PravegaOptionsUtil.getPravegaConfig(tableOptions),
                PravegaOptionsUtil.getSinkStream(tableOptions),
                PravegaOptionsUtil.getWriterMode(tableOptions),
                PravegaOptionsUtil.getTransactionLeaseRenewalIntervalMillis(tableOptions),
                PravegaOptionsUtil.isWatermarkPropagationEnabled(tableOptions),
                PravegaOptionsUtil.getRoutingKeyField(tableOptions));
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
