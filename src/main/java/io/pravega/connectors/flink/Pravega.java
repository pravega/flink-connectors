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
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.connectors.flink.util.StreamWithBoundaries;
import io.pravega.connectors.flink.watermark.AssignerWithTimeWindows;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;

/**
 * Pravega connector descriptor.
 */
public class Pravega extends ConnectorDescriptor {

    public static final String CONNECTOR_TYPE_VALUE_PRAVEGA = "pravega";

    // currently the value is fixed but as we evolve we may have multiple versions to support
    public static final int CONNECTOR_VERSION_VALUE = 1;

    public static final String CONNECTOR_METRICS = "connector.metrics";

    // Connection Configurations
    public static final String CONNECTOR_CONNECTION_CONFIG = "connector.connection-config";
    public static final String CONNECTOR_CONNECTION_CONFIG_CONTROLLER_URI = "connector.connection-config.controller-uri";
    public static final String CONNECTOR_CONNECTION_CONFIG_DEFAULT_SCOPE = "connector.connection-config.default-scope";
    public static final String CONNECTOR_CONNECTION_CONFIG_SECURITY = "connector.connection-config.security";
    public static final String CONNECTOR_CONNECTION_CONFIG_SECURITY_AUTH_TYPE = "connector.connection-config.security.auth-type";
    public static final String CONNECTOR_CONNECTION_CONFIG_SECURITY_AUTH_TOKEN = "connector.connection-config.security.auth-token";
    public static final String CONNECTOR_CONNECTION_CONFIG_SECURITY_VALIDATE_HOSTNAME = "connector.connection-config.security.validate-hostname";
    public static final String CONNECTOR_CONNECTION_CONFIG_SECURITY_TRUST_STORE = "connector.connection-config.security.trust-store";

    // Reader Configurations - STREAM INFO
    public static final String CONNECTOR_READER = "connector.reader";
    public static final String CONNECTOR_READER_STREAM_INFO = "connector.reader.stream-info";
    public static final String CONNECTOR_READER_STREAM_INFO_SCOPE = "scope";
    public static final String CONNECTOR_READER_STREAM_INFO_STREAM = "stream";
    public static final String CONNECTOR_READER_STREAM_INFO_START_STREAMCUT = "start-streamcut";
    public static final String CONNECTOR_READER_STREAM_INFO_END_STREAMCUT = "end-streamcut";

    // Reader Configurations - READER GROUP
    public static final String CONNECTOR_READER_READER_GROUP = "connector.reader.reader-group";
    public static final String CONNECTOR_READER_READER_GROUP_UID = "connector.reader.reader-group.uid";
    public static final String CONNECTOR_READER_READER_GROUP_SCOPE = "connector.reader.reader-group.scope";
    public static final String CONNECTOR_READER_READER_GROUP_NAME = "connector.reader.reader-group.name";
    public static final String CONNECTOR_READER_READER_GROUP_REFRESH_INTERVAL = "connector.reader.reader-group.refresh-interval";
    public static final String CONNECTOR_READER_READER_GROUP_EVENT_READ_TIMEOUT_INTERVAL = "connector.reader.reader-group.event-read-timeout-interval";
    public static final String CONNECTOR_READER_READER_GROUP_CHECKPOINT_INITIATE_TIMEOUT_INTERVAL = "connector.reader.reader-group.checkpoint-initiate-timeout-interval";

    // Reader Configurations - USER
    public static final String CONNECTOR_READER_USER_TIMESTAMP_ASSIGNER = "connector.reader.user.timestamp-assigner";

    // Writer Configurations
    public static final String CONNECTOR_WRITER = "connector.writer";
    public static final String CONNECTOR_WRITER_SCOPE = "connector.writer.scope";
    public static final String CONNECTOR_WRITER_STREAM = "connector.writer.stream";
    public static final String CONNECTOR_WRITER_MODE = "connector.writer.mode";
    public static final String CONNECTOR_WRITER_MODE_VALUE_EXACTLY_ONCE = "exactly_once";
    public static final String CONNECTOR_WRITER_MODE_VALUE_ATLEAST_ONCE = "atleast_once";
    public static final String CONNECTOR_WRITER_TXN_LEASE_RENEWAL_INTERVAL = "connector.writer.txn-lease-renewal-interval";
    public static final String CONNECTOR_WRITER_ENABLE_WATERMARK = "connector.writer.enable-watermark";
    public static final String CONNECTOR_WRITER_ROUTING_KEY_FILED_NAME = "connector.writer.routingkey-field-name";

    private TableSourceReaderBuilder tableSourceReaderBuilder = null;

    private TableSinkWriterBuilder tableSinkWriterBuilder = null;

    public Pravega() {
        super(CONNECTOR_TYPE_VALUE_PRAVEGA, CONNECTOR_VERSION_VALUE, true);
    }

    /**
     * Internal method for connector properties conversion.
     */
    @Override
    protected Map<String, String> toConnectorProperties() {
        final DescriptorProperties properties = new DescriptorProperties();
        properties.putString(CONNECTOR_VERSION, String.valueOf(CONNECTOR_VERSION_VALUE));

        if (tableSourceReaderBuilder == null && tableSinkWriterBuilder == null) {
            throw new ValidationException("Missing both reader and writer configurations.");
        }

        PravegaConfig pravegaConfig = tableSourceReaderBuilder != null ?
                tableSourceReaderBuilder.getPravegaConfig() : tableSinkWriterBuilder.getPravegaConfig();
        populateConnectionConfig(pravegaConfig, properties);

        boolean metrics = tableSourceReaderBuilder != null ?
                tableSourceReaderBuilder.isMetricsEnabled() : tableSinkWriterBuilder.isMetricsEnabled();
        properties.putBoolean(CONNECTOR_METRICS, metrics);

        if (tableSourceReaderBuilder != null) {
            populateReaderProperties(properties);
        }
        if (tableSinkWriterBuilder != null) {
            populateWriterProperties(properties);
        }
        return properties.asMap();
    }

    /**
     * Prepare Pravega connection specific configurations
     * @param pravegaConfig the Pravega configuration to use.
     * @param properties the supplied descriptor properties.
     */
    private void populateConnectionConfig(PravegaConfig pravegaConfig, DescriptorProperties properties) {

        String controllerUri = pravegaConfig.getClientConfig().getControllerURI().toString();
        properties.putString(CONNECTOR_CONNECTION_CONFIG_CONTROLLER_URI, controllerUri);

        String defaultScope = pravegaConfig.getDefaultScope();
        if (defaultScope != null && defaultScope.length() != 0) {
            properties.putString(CONNECTOR_CONNECTION_CONFIG_DEFAULT_SCOPE, defaultScope);
        }

        if (pravegaConfig.getClientConfig().getCredentials() != null) {
            String authType = pravegaConfig.getClientConfig().getCredentials().getAuthenticationType();
            if (authType != null && authType.length() != 0) {
                properties.putString(CONNECTOR_CONNECTION_CONFIG_SECURITY_AUTH_TYPE, authType);
            }
            String authToken = pravegaConfig.getClientConfig().getCredentials().getAuthenticationToken();
            if (authToken != null && authToken.length() != 0) {
                properties.putString(CONNECTOR_CONNECTION_CONFIG_SECURITY_AUTH_TOKEN, authToken);
            }
        }

        boolean validateHostName = pravegaConfig.getClientConfig().isValidateHostName();
        properties.putBoolean(CONNECTOR_CONNECTION_CONFIG_SECURITY_VALIDATE_HOSTNAME, validateHostName);

        String trustStore = pravegaConfig.getClientConfig().getTrustStore();
        if (trustStore != null && trustStore.length() != 0) {
            properties.putString(CONNECTOR_CONNECTION_CONFIG_SECURITY_TRUST_STORE, trustStore);
        }
    }

    /**
     * Populate all the writer configurations based on the values supplied through {@link TableSinkWriterBuilder}
     * @param properties the supplied descriptor properties.
     */
    private void populateWriterProperties(DescriptorProperties properties) {
        properties.putBoolean(CONNECTOR_WRITER, true);
        properties.putString(CONNECTOR_WRITER_SCOPE, tableSinkWriterBuilder.resolveStream().getScope());
        properties.putString(CONNECTOR_WRITER_STREAM, tableSinkWriterBuilder.resolveStream().getStreamName());

        if (tableSinkWriterBuilder.writerMode == PravegaWriterMode.ATLEAST_ONCE) {
            properties.putString(CONNECTOR_WRITER_MODE, CONNECTOR_WRITER_MODE_VALUE_ATLEAST_ONCE);

        } else if (tableSinkWriterBuilder.writerMode == PravegaWriterMode.EXACTLY_ONCE) {
            properties.putString(CONNECTOR_WRITER_MODE, CONNECTOR_WRITER_MODE_VALUE_EXACTLY_ONCE);
        }

        properties.putBoolean(CONNECTOR_WRITER_ENABLE_WATERMARK, tableSinkWriterBuilder.enableWatermark);
        properties.putLong(CONNECTOR_WRITER_TXN_LEASE_RENEWAL_INTERVAL, tableSinkWriterBuilder.txnLeaseRenewalPeriod.toMilliseconds());

        if (tableSinkWriterBuilder.routingKeyFieldName != null) {
            properties.putString(CONNECTOR_WRITER_ROUTING_KEY_FILED_NAME, tableSinkWriterBuilder.routingKeyFieldName);
        }
    }

    /**
     * Populate all the reader configurations based on the values supplied through {@link TableSourceReaderBuilder}
     * @param properties the supplied descriptor properties.
     */
    private void populateReaderProperties(DescriptorProperties properties) {
        properties.putBoolean(CONNECTOR_READER, true);

        // reader stream information
        AbstractStreamingReaderBuilder.ReaderGroupInfo readerGroupInfo = tableSourceReaderBuilder.buildReaderGroupInfo();

        Map<Stream, StreamCut> startStreamCuts = readerGroupInfo.getReaderGroupConfig().getStartingStreamCuts();
        Map<Stream, StreamCut> endStreamCuts = readerGroupInfo.getReaderGroupConfig().getEndingStreamCuts();
        final List<List<String>> values = new ArrayList<>();
        startStreamCuts.keySet().stream().forEach(stream -> {
            StreamCut startStreamCut = startStreamCuts.get(stream);
            StreamCut endStreamCut = endStreamCuts.get(stream);
            values.add(Arrays.asList(stream.getScope(), stream.getStreamName(), startStreamCut.asText(), endStreamCut.asText()));
        });
        properties.putIndexedFixedProperties(
                                                CONNECTOR_READER_STREAM_INFO,
                                                Arrays.asList(
                                                            CONNECTOR_READER_STREAM_INFO_SCOPE,
                                                            CONNECTOR_READER_STREAM_INFO_STREAM,
                                                            CONNECTOR_READER_STREAM_INFO_START_STREAMCUT,
                                                            CONNECTOR_READER_STREAM_INFO_END_STREAMCUT
                                                        ),
                                                values
                                            );

        // reader group information
        String uid = Optional.ofNullable(tableSourceReaderBuilder.uid).orElseGet(tableSourceReaderBuilder::generateUid);
        properties.putString(CONNECTOR_READER_READER_GROUP_UID, uid);
        properties.putString(CONNECTOR_READER_READER_GROUP_SCOPE, readerGroupInfo.getReaderGroupScope());
        properties.putString(CONNECTOR_READER_READER_GROUP_NAME, readerGroupInfo.getReaderGroupName());
        properties.putLong(CONNECTOR_READER_READER_GROUP_REFRESH_INTERVAL, readerGroupInfo.getReaderGroupConfig().getGroupRefreshTimeMillis());
        properties.putLong(CONNECTOR_READER_READER_GROUP_EVENT_READ_TIMEOUT_INTERVAL, tableSourceReaderBuilder.eventReadTimeout.toMilliseconds());
        properties.putLong(CONNECTOR_READER_READER_GROUP_CHECKPOINT_INITIATE_TIMEOUT_INTERVAL, tableSourceReaderBuilder.checkpointInitiateTimeout.toMilliseconds());

        // user information
        if (tableSourceReaderBuilder.getAssignerWithTimeWindows() != null) {
            try {
                @SuppressWarnings("unchecked")
                AssignerWithTimeWindows<Row> assigner = (AssignerWithTimeWindows<Row>) tableSourceReaderBuilder
                        .getAssignerWithTimeWindows().deserializeValue(getClass().getClassLoader());

                properties.putClass(CONNECTOR_READER_USER_TIMESTAMP_ASSIGNER, assigner.getClass());
            } catch (Exception e) {
                throw new TableException(e.getMessage());
            }
        }
    }

    public TableSourceReaderBuilder tableSourceReaderBuilder() {
        tableSourceReaderBuilder = new TableSourceReaderBuilder();
        return tableSourceReaderBuilder;
    }

    public TableSinkWriterBuilder tableSinkWriterBuilder() {
        tableSinkWriterBuilder = new TableSinkWriterBuilder();
        return tableSinkWriterBuilder;
    }

    /**
     * Reader builder which can be used to define the Pravega reader configurations. The supplied configurations will be used
     * to create appropriate Table source implementation.
     *
     */
    public static class TableSourceReaderBuilder<T extends AbstractStreamingReaderBuilder>
            extends AbstractStreamingReaderBuilder<Row, TableSourceReaderBuilder> {

        private DeserializationSchema<Row> deserializationSchema;
        private SerializedValue<AssignerWithTimeWindows<Row>> assignerWithTimeWindows;

        @Override
        protected DeserializationSchema<Row> getDeserializationSchema() {
            return this.deserializationSchema;
        }

        @Override
        protected SerializedValue<AssignerWithTimeWindows<Row>> getAssignerWithTimeWindows() {
            return this.assignerWithTimeWindows;
        }

        @Override
        protected TableSourceReaderBuilder builder() {
            return this;
        }

        /**
         * Pass the deserialization schema to be used.
         * @param deserializationSchema the deserialization schema.
         * @return TableSourceReaderBuilder instance.
         */
        protected TableSourceReaderBuilder withDeserializationSchema(DeserializationSchema<Row> deserializationSchema) {
            this.deserializationSchema = deserializationSchema;
            return this;
        }

        /**
         * Configures the timestamp and watermark assigner.
         *
         * @param assignerWithTimeWindows the timestamp and watermark assigner.
         * @return TableSourceReaderBuilder instance.
         */
        // TODO: Due to the serialization validation for `connectorProperties`, only `public` `static-inner/outer` class implements
        // `AssignerWithTimeWindow` is supported as a parameter of `withTimestampAssigner` in Table API stream table source.
        protected TableSourceReaderBuilder withTimestampAssigner(AssignerWithTimeWindows<Row> assignerWithTimeWindows) {
            try {
                ClosureCleaner.clean(assignerWithTimeWindows, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
                this.assignerWithTimeWindows = new SerializedValue<>(assignerWithTimeWindows);
            } catch (IOException e) {
                throw new IllegalArgumentException("The given assigner is not serializable", e);
            }
            return this;
        }

        /**
         * factory to build an {@link FlinkPravegaInputFormat} for using Table API in a batch environment.
         * @return a supplier to eagerly validate the configuration and lazily construct the input format.
         */
        FlinkPravegaInputFormat<Row> buildInputFormat() {
            Preconditions.checkState(deserializationSchema != null, "The deserializationSchema must be provided.");
            final List<StreamWithBoundaries> streams = resolveStreams();
            final ClientConfig clientConfig = getPravegaConfig().getClientConfig();
            return new FlinkPravegaInputFormat<>(clientConfig, streams, deserializationSchema);
        }
    }

    /**
     * Writer builder which can be used to define the Pravega writer configurations. The supplied configurations will be used
     * to create appropriate Table sink implementation.
     *
     */
    public static class TableSinkWriterBuilder<T extends AbstractStreamingWriterBuilder>
            extends AbstractStreamingWriterBuilder<Row, TableSinkWriterBuilder> {

        private String routingKeyFieldName;

        private SerializationSchema<Row> serializationSchema;

        /**
         * Sets the field name to use as a Pravega event routing key.
         * @param fieldName the field name.
         */
        public TableSinkWriterBuilder withRoutingKeyField(String fieldName) {
            this.routingKeyFieldName = fieldName;
            return builder();
        }

        /**
         * Pass the serialization schema to be used.
         * @param serializationSchema the serialization schema.
         */
        public TableSinkWriterBuilder withSerializationSchema(SerializationSchema<Row> serializationSchema) {
            this.serializationSchema = serializationSchema;
            return builder();
        }

        @Override
        protected TableSinkWriterBuilder builder() {
            return this;
        }

        /**
         * Creates the sink function based on the given table sink configuration and current builder state.
         * @param configuration the table sink configuration, incl. projected fields
         */
        protected FlinkPravegaWriter<Row> createSinkFunction(FlinkPravegaTableSink.TableSinkConfiguration configuration) {
            Preconditions.checkState(routingKeyFieldName != null, "The routing key field must be provided.");
            Preconditions.checkState(serializationSchema != null, "The serializationSchema must be provided.");
            PravegaEventRouter<Row> eventRouter = new FlinkPravegaTableSink.RowBasedRouter(routingKeyFieldName, configuration.getFieldNames(), configuration.getFieldTypes());
            return createSinkFunction(serializationSchema, eventRouter);
        }

        /**
         * Creates FlinkPravegaOutputFormat based on the given table sink configuration and current builder state.
         * @param configuration the table sink configuration, incl. projected fields
         */
        protected FlinkPravegaOutputFormat<Row> createOutputFormat(FlinkPravegaTableSink.TableSinkConfiguration configuration) {
            Preconditions.checkState(routingKeyFieldName != null, "The routing key field must be provided.");
            Preconditions.checkState(serializationSchema != null, "The serializationSchema must be provided.");
            PravegaEventRouter<Row> eventRouter = new FlinkPravegaTableSink.RowBasedRouter(routingKeyFieldName, configuration.getFieldNames(), configuration.getFieldTypes());
            return new FlinkPravegaOutputFormat<>(
                    getPravegaConfig().getClientConfig(),
                    resolveStream(),
                    serializationSchema,
                    eventRouter
            );
        }

    }
}