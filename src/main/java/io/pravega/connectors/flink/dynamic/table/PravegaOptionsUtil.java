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

import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.PravegaWriterMode;
import io.pravega.connectors.flink.util.FlinkPravegaUtils;
import io.pravega.connectors.flink.util.StreamWithBoundaries;
import io.pravega.shared.NameUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
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
import static io.pravega.connectors.flink.util.FlinkPravegaUtils.isCredentialsLoadDynamic;

/** Utilities for {@link PravegaOptions}. */
@Internal
public class PravegaOptionsUtil {

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

    private PravegaOptionsUtil() {}

    // --------------------------------------------------------------------------------------------
    // Validation
    // --------------------------------------------------------------------------------------------

    public static void validateTableSourceOptions(ReadableConfig tableOptions) {
        validateScanExecutionType(tableOptions);
        validateSourceStreams(tableOptions);
        if (tableOptions.get(SCAN_EXECUTION_TYPE).equals(SCAN_EXECUTION_TYPE_VALUE_STREAMING)) {
            validateReaderGroupConfig(tableOptions);
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

    private static void validateReaderGroupConfig(ReadableConfig tableOptions) {
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
                .withControllerURI(URI.create(tableOptions.get(CONTROLLER_URI)))
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

    public static String getUid(ReadableConfig tableOptions) {
        return tableOptions.get(SCAN_UID);
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

    public static String getRoutingKeyField(ReadableConfig tableOptions) {
        return tableOptions.get(SINK_ROUTINGKEY_FIELD_NAME);
    }


}
