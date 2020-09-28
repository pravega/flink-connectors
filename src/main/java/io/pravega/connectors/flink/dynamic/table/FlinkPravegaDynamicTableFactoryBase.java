package io.pravega.connectors.flink.dynamic.table;

import io.pravega.client.stream.StreamCut;
import io.pravega.connectors.flink.FlinkPravegaInputFormat;
import io.pravega.connectors.flink.FlinkPravegaOutputFormat;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.table.descriptors.Pravega;
import io.pravega.connectors.flink.table.descriptors.PravegaValidator;
import io.pravega.connectors.flink.util.ConnectorConfigurations;
import io.pravega.connectors.flink.util.StreamWithBoundaries;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.*;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.pravega.connectors.flink.dynamic.table.PravegaOptions.*;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;

public abstract class FlinkPravegaDynamicTableFactoryBase implements
        DynamicTableSourceFactory,
        DynamicTableSinkFactory {


    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        ReadableConfig tableOptions = helper.getOptions();

        EncodingFormat<SerializationSchema<RowData>> encodingFormat = helper.discoverEncodingFormat(
                SerializationFormatFactory.class,
                FactoryUtil.FORMAT);
        final DescriptorProperties descriptorProperties = getValidatedProperties(context.getCatalogTable().getOptions());
        final TableSchema schema = TableSchemaUtils.getPhysicalSchema(descriptorProperties.getTableSchema(SCHEMA));
        SerializationSchema<RowData> serializationSchema = getSerializationSchema(context.getCatalogTable().getOptions());

        ConnectorConfigurations connectorConfigurations = new ConnectorConfigurations();
        connectorConfigurations.parseConfigurations(descriptorProperties, ConnectorConfigurations.ConfigurationType.WRITER);

        Pravega.DynamicTableSinkWriterBuilder tableSinkWriterBuilder = new Pravega().dynamicTableSinkWriterBuilder();
        if (connectorConfigurations.getTxnLeaseRenewalInterval().isPresent()) {
            tableSinkWriterBuilder.withTxnLeaseRenewalPeriod(Time.milliseconds(connectorConfigurations.getTxnLeaseRenewalInterval().get().longValue()));
        }
        if (connectorConfigurations.getWriterMode().isPresent()) {
            tableSinkWriterBuilder.withWriterMode(connectorConfigurations.getWriterMode().get());
        }
        if (connectorConfigurations.getMetrics().isPresent()) {
            tableSinkWriterBuilder.enableMetrics(connectorConfigurations.getMetrics().get());
        }
        if (connectorConfigurations.getWatermark().isPresent()) {
            tableSinkWriterBuilder.enableWatermark(connectorConfigurations.getWatermark().get());
        }
        tableSinkWriterBuilder.withPravegaConfig(connectorConfigurations.getPravegaConfig());
        tableSinkWriterBuilder.withRoutingKeyField(connectorConfigurations.getRoutingKey());
        tableSinkWriterBuilder.withSerializationSchema(serializationSchema);
        tableSinkWriterBuilder.forStream(connectorConfigurations.getWriterStream());
        tableSinkWriterBuilder.withPravegaConfig(connectorConfigurations.getPravegaConfig());

        return createFlinkPravegaTableSink(
                schema,
                tableSinkWriterBuilder::createSinkFunction,
                tableSinkWriterBuilder::createOutputFormat,
                encodingFormat);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        ReadableConfig tableOptions = helper.getOptions();

        DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(
                DeserializationFormatFactory.class,
                FactoryUtil.FORMAT);
        final DescriptorProperties descriptorProperties = getValidatedProperties(context.getCatalogTable().getOptions());
        final TableSchema schema = TableSchemaUtils.getPhysicalSchema(descriptorProperties.getTableSchema(SCHEMA));
        final DeserializationSchema<RowData> deserializationSchema = getDeserializationSchema(context.getCatalogTable().getOptions());

        ConnectorConfigurations connectorConfigurations = new ConnectorConfigurations();
        connectorConfigurations.parseConfigurations(descriptorProperties, ConnectorConfigurations.ConfigurationType.READER);

        // create source from the reader builder by using the supplied properties
        Pravega.DynamicTableSourceReaderBuilder tableSourceReaderBuilder = new Pravega().dynamicTableSourceReaderBuilder();
        tableSourceReaderBuilder.withDeserializationSchema(deserializationSchema);

        if (connectorConfigurations.getAssignerWithTimeWindows().isPresent()) {
            tableSourceReaderBuilder.withTimestampAssigner(connectorConfigurations.getAssignerWithTimeWindows().get());
        }

        if (connectorConfigurations.getUid().isPresent()) {
            tableSourceReaderBuilder.uid(connectorConfigurations.getUid().get());
        }
        if (connectorConfigurations.getRgScope().isPresent()) {
            tableSourceReaderBuilder.withReaderGroupScope(connectorConfigurations.getRgScope().get());
        }
        if (connectorConfigurations.getRgName().isPresent()) {
            tableSourceReaderBuilder.withReaderGroupName(connectorConfigurations.getRgName().get());
        }
        if (connectorConfigurations.getRefreshInterval().isPresent()) {
            tableSourceReaderBuilder.withReaderGroupRefreshTime(Time.milliseconds(connectorConfigurations.getRefreshInterval().get()));
        }
        if (connectorConfigurations.getEventReadTimeoutInterval().isPresent()) {
            tableSourceReaderBuilder.withEventReadTimeout(Time.milliseconds(connectorConfigurations.getEventReadTimeoutInterval().get()));
        }
        if (connectorConfigurations.getCheckpointInitiateTimeoutInterval().isPresent()) {
            tableSourceReaderBuilder.withCheckpointInitiateTimeout(Time.milliseconds(connectorConfigurations.getCheckpointInitiateTimeoutInterval().get()));
        }

        tableSourceReaderBuilder.withPravegaConfig(connectorConfigurations.getPravegaConfig());
        if (connectorConfigurations.getMetrics().isPresent()) {
            tableSourceReaderBuilder.enableMetrics(connectorConfigurations.getMetrics().get());
        }
        tableSourceReaderBuilder.withPravegaConfig(connectorConfigurations.getPravegaConfig());
        for (StreamWithBoundaries streamWithBoundaries: connectorConfigurations.getReaderStreams()) {
            if (streamWithBoundaries.getFrom() != StreamCut.UNBOUNDED && streamWithBoundaries.getTo() != StreamCut.UNBOUNDED) {
                tableSourceReaderBuilder.forStream(streamWithBoundaries.getStream(), streamWithBoundaries.getFrom(), streamWithBoundaries.getTo());
            } else if (streamWithBoundaries.getFrom() != StreamCut.UNBOUNDED) {
                tableSourceReaderBuilder.forStream(streamWithBoundaries.getStream(), streamWithBoundaries.getFrom());
            } else {
                tableSourceReaderBuilder.forStream(streamWithBoundaries.getStream());
            }
        }

        return createFlinkPravegaTableSource(
                tableSourceReaderBuilder::buildSourceFunction,
                tableSourceReaderBuilder::buildInputFormat,
                schema,
                decodingFormat);
    }

    protected FlinkPravegaDynamicTableSink createFlinkPravegaTableSink(
            TableSchema schema,
            Function<TableSchema, FlinkPravegaWriter<RowData>> writerFactory,
            Function<TableSchema, FlinkPravegaOutputFormat<RowData>> outputFormatFactory,
            EncodingFormat<SerializationSchema<RowData>> encodingFormat) {
        return new FlinkPravegaDynamicTableSink(
                schema,
                writerFactory,
                outputFormatFactory,
                encodingFormat);
    }

    protected FlinkPravegaDynamicTableSource createFlinkPravegaTableSource(
            Supplier<FlinkPravegaReader<RowData>> sourceFunctionFactory,
            Supplier<FlinkPravegaInputFormat<RowData>> inputFormatFactory,
            TableSchema schema,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat) {
        return new FlinkPravegaDynamicTableSource(
                sourceFunctionFactory,
                inputFormatFactory,
                schema,
                decodingFormat);
    }

    protected DescriptorProperties getValidatedProperties(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);

        boolean supportsSourceTimestamps = true;
        boolean supportsSourceWatermarks = true;
        new SchemaValidator(isStreamEnvironment(), supportsSourceTimestamps, supportsSourceWatermarks).validate(descriptorProperties);

        new PravegaValidator().validate(descriptorProperties);

        return descriptorProperties;
    }

    protected SerializationSchema<RowData> getSerializationSchema(Map<String, String> properties) {
        @SuppressWarnings("unchecked")
        final SerializationSchemaFactory<RowData> formatFactory = TableFactoryService.find(
                SerializationSchemaFactory.class,
                properties,
                this.getClass().getClassLoader());
        return formatFactory.createSerializationSchema(properties);
    }

    protected DeserializationSchema<RowData> getDeserializationSchema(Map<String, String> properties) {
        @SuppressWarnings("unchecked")
        final DeserializationSchemaFactory<RowData> formatFactory = TableFactoryService.find(
                DeserializationSchemaFactory.class,
                properties,
                this.getClass().getClassLoader());
        return formatFactory.createDeserializationSchema(properties);
    }

    protected abstract boolean isStreamEnvironment();

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(CONNECTION_CONTROLLER_URL);
        options.add(CONNECTION_DEFAULT_SCOPE);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(CONNECTION_SECURITY);
        options.add(CONNECTION_SECURITY_AUTH_TYPE);
        options.add(CONNECTION_SECURITY_AUTH_TOKEN);
        options.add(CONNECTION_SECURITY_VALIDATE_HOSTNAME);
        options.add(CONNECTION_SECURITY_TRUST_STORE);
        options.add(SCAN);
        options.add(SCAN_STREAM_INFO);
        options.add(SCAN_SCOPE);
        options.add(SCAN_STREAM);
        options.add(SCAN_START_STREAMCUT);
        options.add(SCAN_END_STREAMCUT);
        options.add(SCAN_READER_GROUP);
        options.add(SCAN_READER_GROUP_UID);
        options.add(SCAN_READER_GROUP_SCOPE);
        options.add(SCAN_READER_GROUP_NAME);
        options.add(SCAN_READER_GROUP_REFRESH_INTERVAL);
        options.add(SCAN_READER_GROUP_EVENT_READ_TIMEOUT_INTERVAL);
        options.add(SCAN_READER_GROUP_CHECKPOINT_INITIATE_TIMEOUT_INTERVAL);
        options.add(SINK);
        options.add(SINK_SCOPE);
        options.add(SINK_STREAM);
        options.add(SINK_MODE);
        options.add(EXACTLY_ONCE);
        options.add(ATLEAST_ONCE);
        options.add(SINK_TXN_LEASE_RENEWAL_INTERVAL);
        options.add(SINK_ENABLE_WATERMARK);
        options.add(SINK_ROUTINGKEY_FIELD_NAME);
        return options;
    }
}
