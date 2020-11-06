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
import io.pravega.client.stream.StreamCut;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.PravegaWriterMode;
import io.pravega.connectors.flink.util.FlinkPravegaUtils;
import io.pravega.connectors.flink.util.StreamWithBoundaries;
import io.pravega.shared.NameUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.pravega.connectors.flink.util.FlinkPravegaUtils.isCredentialsLoadDynamic;

public class PravegaOptions {
    // --------------------------------------------------------------------------------------------
    // Connection specific options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<String> CONTROLLER_URL = ConfigOptions
            .key("controller-uri")
            .stringType()
            .noDefaultValue()
            .withDescription("Required Pravega controller URI");

    public static final ConfigOption<String> SCOPE = ConfigOptions
            .key("scope")
            .stringType()
            .noDefaultValue()
            .withDescription("Required default scope name");

    public static final ConfigOption<String> SECURITY_AUTH_TYPE = ConfigOptions
            .key("security.auth-type")
            .stringType()
            .noDefaultValue()
            .withDescription("Optional static authentication/authorization type for security");

    public static final ConfigOption<String> SECURITY_AUTH_TOKEN = ConfigOptions
            .key("security.auth-token")
            .stringType()
            .noDefaultValue()
            .withDescription("Optional static authentication/authorization token for security");

    public static final ConfigOption<Boolean> SECURITY_VALIDATE_HOSTNAME = ConfigOptions
            .key("security.validate-hostname")
            .booleanType()
            .defaultValue(true)
            .withDescription("Optional flag to decide whether to enable host name validation when TLS is enabled");

    public static final ConfigOption<String> SECURITY_TRUST_STORE = ConfigOptions
            .key("security.trust-store")
            .stringType()
            .noDefaultValue()
            .withDescription("Optional trust store for Pravega client");

    // --------------------------------------------------------------------------------------------
    // Scan specific options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<String> SCAN_EXECUTION_TYPE = ConfigOptions
            .key("scan.execution.type")
            .stringType()
            .defaultValue("streaming")
            .withDescription("Optional execution type. Valid enumerations are ['streaming'(default), 'batch']");

    public static final ConfigOption<String> SCAN_READER_GROUP_NAME = ConfigOptions
            .key("scan.reader-group.name")
            .stringType()
            .noDefaultValue()
            .withDescription("Required Pravega reader group name");

    public static final ConfigOption<List<String>> SCAN_STREAMS = ConfigOptions
            .key("scan.streams")
            .stringType()
            .asList()
            .noDefaultValue()
            .withDescription("Required semicolon-separated list of stream names from which the table is read");

    public static final ConfigOption<List<String>> SCAN_START_STREAMCUTS = ConfigOptions
            .key("scan.start-streamcuts")
            .stringType()
            .asList()
            .noDefaultValue()
            .withDescription("Optional semicolon-separated list of base64 encoded strings for start streamcuts, " +
                    "should be ordered as 'scan.streams', by default setting all to the beginning of the stream");

    public static final ConfigOption<List<String>> SCAN_END_STREAMCUTS = ConfigOptions
            .key("scan.end-streamcuts")
            .stringType()
            .asList()
            .noDefaultValue()
            .withDescription("Optional semicolon-separated list of base64 encoded strings for end streamcuts, " +
                    " should be ordered as 'scan.streams', by default setting all to the unbounded end of the stream");

    public static final ConfigOption<String> SCAN_UID = ConfigOptions
            .key("scan.uid")
            .stringType()
            .noDefaultValue()
            .withDescription("Optional uid for the table source operator to identify the checkpoint state");

    public static final ConfigOption<Integer> SCAN_READER_GROUP_MAX_OUTSTANDING_CHECKPOINT_REQUEST = ConfigOptions
            .key("scan.reader-group.max-outstanding-checkpoint-request")
            .intType()
            .defaultValue(3)
            .withDescription("Optional maximum outstanding checkpoint requests to Pravega (default=3)");

    public static final ConfigOption<Duration> SCAN_READER_GROUP_REFRESH_INTERVAL = ConfigOptions
            .key("scan.reader-group.refresh.interval")
            .durationType()
            .defaultValue(Duration.ofSeconds(3))
            .withDescription("Optional refresh interval for reader group (default=3s)");

    public static final ConfigOption<Duration> SCAN_EVENT_READ_TIMEOUT_INTERVAL = ConfigOptions
            .key("scan.event-read.timeout.interval")
            .durationType()
            .defaultValue(Duration.ofSeconds(1))
            .withDescription("Optional timeout for the call to read events from Pravega (default=1s)");

    public static final ConfigOption<Duration> SCAN_READER_GROUP_CHECKPOINT_INITIATE_TIMEOUT_INTERVAL = ConfigOptions
            .key("scan.reader-group.checkpoint-initiate-timeout.interval")
            .durationType()
            .defaultValue(Duration.ofSeconds(5))
            .withDescription("Optional timeout for call that initiates the Pravega checkpoint (default=5s)");

    // --------------------------------------------------------------------------------------------
    // Sink specific options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<String> SINK_STREAM = ConfigOptions
            .key("sink.stream")
            .stringType()
            .noDefaultValue()
            .withDescription("Required stream name to which the table is written");

    public static final ConfigOption<String> SINK_SEMANTIC = ConfigOptions
            .key("sink.semantic")
            .stringType()
            .defaultValue("at-least-once")
            .withDescription("Optional semantic when commit. Valid enumerations are ['at-least-once'(default), 'exactly-once', 'best-effort']");

    public static final ConfigOption<Duration> SINK_TXN_LEASE_RENEWAL_INTERVAL = ConfigOptions
            .key("sink.txn-lease-renewal.interval")
            .durationType()
            .defaultValue(Duration.ofSeconds(30))
            .withDescription("Optional transaction lease renewal period, valid for exactly-once semantic");

    public static final ConfigOption<Boolean> SINK_ENABLE_WATERMARK_PROPAGATION = ConfigOptions
            .key("sink.enable.watermark-propagation")
            .booleanType()
            .defaultValue(false)
            .withDescription("Optional flag to enable watermark propagation from Flink table to Pravega stream");

    public static final ConfigOption<String> SINK_ROUTINGKEY_FIELD_NAME = ConfigOptions
            .key("sink.routing-key.field.name")
            .stringType()
            .noDefaultValue()
            .withDescription("Optional field name to use as a Pravega event routing key, field type must be STRING, random routing if not specified");

    // --------------------------------------------------------------------------------------------
    // Option enumerations
    // --------------------------------------------------------------------------------------------

    public static final String SCAN_EXECUTION_TYPE_VALUE_STREAMING = "streaming";
    public static final String SCAN_EXECUTION_TYPE_VALUE_BATCH = "batch";

    public static final String SINK_SEMANTIC_VALUE_EXACTLY_ONCE = "exactly-once";
    public static final String SINK_SEMANTIC_VALUE_AT_LEAST_ONCE = "at-least-once";
    public static final String SINK_SEMANTIC_VALUE_BEST_EFFORT = "best-effort";

    private static final Set<String> SCAN_EXECUTION_TYPE_ENUMS = new HashSet<>(Arrays.asList(
            SCAN_EXECUTION_TYPE_VALUE_STREAMING,
            SCAN_EXECUTION_TYPE_VALUE_BATCH
    ));

    private static final Set<String> SINK_SEMANTIC_ENUMS = new HashSet<>(Arrays.asList(
            SINK_SEMANTIC_VALUE_AT_LEAST_ONCE,
            SINK_SEMANTIC_VALUE_EXACTLY_ONCE,
            SINK_SEMANTIC_VALUE_BEST_EFFORT
    ));

    private PravegaOptions() {}

    // --------------------------------------------------------------------------------------------
    // Validation
    // --------------------------------------------------------------------------------------------

    public static void validateTableSourceOptions(ReadableConfig tableOptions) {
        validateScanExecutionType(tableOptions);
        validateSourceStreams(tableOptions);
        if (tableOptions.get(SCAN_EXECUTION_TYPE).equals(SCAN_EXECUTION_TYPE_VALUE_STREAMING)) {
            validateReaderGroup(tableOptions);
        }
    }

    public static void validateTableSinkOptions(ReadableConfig tableOptions) {
        validateSinkStream(tableOptions);
        validateSinkSemantic(tableOptions);
    }

    private static void validateScanExecutionType(ReadableConfig tableOptions) {
        tableOptions.getOptional(SCAN_EXECUTION_TYPE).ifPresent(type -> {
            if (!SCAN_EXECUTION_TYPE_ENUMS.contains(type)) {
                throw new ValidationException(
                        String.format("Unsupported value '%s' for '%s'. Supported values are ['streaming', 'batch'].",
                                type, SCAN_EXECUTION_TYPE.key()));
            }
        });
    }

    private static void validateSourceStreams(ReadableConfig tableOptions) {
        List<String> streams = tableOptions.getOptional(SCAN_STREAMS)
                .orElseThrow(() -> new ValidationException(String.format("'%s' is required but missing", SCAN_STREAMS.key())));

        streams.forEach(NameUtils::validateStreamName);

        tableOptions.getOptional(SCAN_START_STREAMCUTS).ifPresent(streamCuts -> {
            if (streamCuts.size() != streams.size()) {
                throw new ValidationException(
                        String.format("Start stream cuts are not matching the number of streams, having %d, expected %d",
                                streamCuts.size(), streams.size()));
            }
        });

        tableOptions.getOptional(SCAN_END_STREAMCUTS).ifPresent(streamCuts -> {
            if (streamCuts.size() != streams.size()) {
                throw new ValidationException(
                        String.format("End stream cuts are not matching the number of streams, having %d, expected %d",
                                streamCuts.size(), streams.size()));
            }
        });
    }

    private static void validateReaderGroup(ReadableConfig tableOptions) {
        Optional<String> readerGroupName = tableOptions.getOptional(SCAN_READER_GROUP_NAME);
        if (!readerGroupName.isPresent()) {
            throw new ValidationException(String.format("'%s' is required but missing", SCAN_READER_GROUP_NAME.key()));
        } else {
            NameUtils.validateReaderGroupName(readerGroupName.get());
        }

        tableOptions.getOptional(SCAN_READER_GROUP_MAX_OUTSTANDING_CHECKPOINT_REQUEST).ifPresent(num -> {
            if (num < 1) {
                throw new ValidationException(String.format("'%s' requires a positive integer, received %d",
                        SCAN_READER_GROUP_MAX_OUTSTANDING_CHECKPOINT_REQUEST.key(), num));
            }
        });
    }

    private static void validateSinkSemantic(ReadableConfig tableOptions) {
        tableOptions.getOptional(SINK_SEMANTIC).ifPresent(semantic -> {
            if (!SINK_SEMANTIC_ENUMS.contains(semantic)) {
                throw new ValidationException(
                        String.format("Unsupported value '%s' for '%s'. Supported values are ['at-least-once', 'exactly-once', 'best-effort'].",
                                semantic, SINK_SEMANTIC.key()));
            }
        });
    }

    private static void validateSinkStream(ReadableConfig tableOptions) {
        String stream = tableOptions.getOptional(SINK_STREAM)
                .orElseThrow(() -> new ValidationException(String.format("'%s' is required but missing", SINK_STREAM.key())));
        NameUtils.validateStreamName(stream);
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    // ------------------------------------- Common ----------------------------------------

    public static PravegaConfig getPravegaConfig(ReadableConfig tableOptions) {
        PravegaConfig pravegaConfig = PravegaConfig.fromDefaults()
                .withControllerURI(URI.create(tableOptions.get(CONTROLLER_URL)))
                .withDefaultScope(tableOptions.get(SCOPE))
                .withHostnameValidation(tableOptions.get(SECURITY_VALIDATE_HOSTNAME))
                .withTrustStore(tableOptions.get(SECURITY_TRUST_STORE));

        Optional<String> authType = tableOptions.getOptional(SECURITY_AUTH_TYPE);
        Optional<String> authToken = tableOptions.getOptional(SECURITY_AUTH_TOKEN);
        if (authType.isPresent() && authToken.isPresent() && !isCredentialsLoadDynamic()) {
            pravegaConfig.withCredentials(new FlinkPravegaUtils.SimpleCredentials(authType.get(), authToken.get()));
        }

        return pravegaConfig;
    }

    // ------------------------------------- Reader ----------------------------------------

    public static boolean isStreamingReader(ReadableConfig tableOptions) {
        return tableOptions.get(SCAN_EXECUTION_TYPE).equals(SCAN_EXECUTION_TYPE_VALUE_STREAMING);
    }

    public static String getReaderGroupName(ReadableConfig tableOptions) {
        return tableOptions.get(SCAN_READER_GROUP_NAME);
    }

    public static Optional<String> getUid(ReadableConfig tableOptions) {
        return tableOptions.getOptional(SCAN_UID);
    }

    public static long getReaderGroupRefreshTimeMillis(ReadableConfig tableOptions) {
        return tableOptions.get(SCAN_READER_GROUP_REFRESH_INTERVAL).toMillis();
    }

    public static long getCheckpointInitiateTimeoutMillis(ReadableConfig tableOptions) {
        return tableOptions.get(SCAN_READER_GROUP_CHECKPOINT_INITIATE_TIMEOUT_INTERVAL).toMillis();
    }

    public static long getEventReadTimeoutMillis(ReadableConfig tableOptions) {
        return tableOptions.get(SCAN_EVENT_READ_TIMEOUT_INTERVAL).toMillis();
    }

    public static int getMaxOutstandingCheckpointRequest(ReadableConfig tableOptions) {
        return tableOptions.get(SCAN_READER_GROUP_MAX_OUTSTANDING_CHECKPOINT_REQUEST);
    }

    public static boolean isBoundedRead(ReadableConfig tableOptions) {
        Optional<List<String>> endStreamCuts = tableOptions.getOptional(SCAN_END_STREAMCUTS);
        return endStreamCuts.isPresent() &&
                endStreamCuts.get().stream().noneMatch(cut -> cut.equals(StreamCut.UNBOUNDED.asText()));
    }

    public static List<StreamWithBoundaries> resolveScanStreams(ReadableConfig tableOptions) {
        String scope = tableOptions.get(SCOPE);
        List<String> streams = tableOptions.getOptional(SCAN_STREAMS)
                .orElseThrow(() -> new TableException("Validator should have checked that"));
        List<String> startStreamCuts = tableOptions.get(SCAN_START_STREAMCUTS);
        List<String> endStreamCuts = tableOptions.get(SCAN_END_STREAMCUTS);
        List<StreamWithBoundaries> result = new ArrayList<>();

        for (int i = 0; i < streams.size(); i++) {
            Stream stream = Stream.of(scope, streams.get(i));
            StreamCut from = startStreamCuts == null ? StreamCut.UNBOUNDED : StreamCut.from(startStreamCuts.get(i));
            StreamCut to = endStreamCuts == null ? StreamCut.UNBOUNDED : StreamCut.from(endStreamCuts.get(i));
            result.add(new StreamWithBoundaries(stream, from, to));
        }

        return result;
    }

    // ------------------------------------- Writer ----------------------------------------

    public static Stream getSinkStream(ReadableConfig tableOptions) {
        String scope = tableOptions.get(SCOPE);
        String stream = tableOptions.get(SINK_STREAM);
        return Stream.of(scope, stream);
    }

    public static PravegaWriterMode getWriterMode(ReadableConfig tableOptions) {
        switch (tableOptions.get(SINK_SEMANTIC)) {
            case SINK_SEMANTIC_VALUE_EXACTLY_ONCE:
                return PravegaWriterMode.EXACTLY_ONCE;
            case SINK_SEMANTIC_VALUE_AT_LEAST_ONCE:
                return PravegaWriterMode.ATLEAST_ONCE;
            case SINK_SEMANTIC_VALUE_BEST_EFFORT:
                return PravegaWriterMode.BEST_EFFORT;
            default:
                throw new TableException("Validator should have checked that");
        }
    }

    public static long getTransactionLeaseRenewalIntervalMillis(ReadableConfig tableOptions) {
        return tableOptions.get(SINK_TXN_LEASE_RENEWAL_INTERVAL).toMillis();
    }

    public static boolean isWatermarkPropagationEnabled(ReadableConfig tableOptions) {
        return tableOptions.get(SINK_ENABLE_WATERMARK_PROPAGATION);
    }

    public static Optional<String> getRoutingKeyField(ReadableConfig tableOptions) {
        return tableOptions.getOptional(SINK_ROUTINGKEY_FIELD_NAME);
    }
}
