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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;
import java.util.List;

public class PravegaOptions {
    // --------------------------------------------------------------------------------------------
    // Connection specific options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<String> CONTROLLER_URI = ConfigOptions
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

    private PravegaOptions() {}
}
