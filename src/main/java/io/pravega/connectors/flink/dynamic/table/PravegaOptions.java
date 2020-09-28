package io.pravega.connectors.flink.dynamic.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class PravegaOptions {
    private PravegaOptions() {}

    public static final ConfigOption<String> CONNECTION = ConfigOptions
            .key("connection")
            .stringType()
            .noDefaultValue()
            .withDescription("connection");

    public static final ConfigOption<String> CONNECTION_CONTROLLER_URL = ConfigOptions
            .key("connection.controller-uri")
            .stringType()
            .noDefaultValue()
            .withDescription("connection.controller-uri");

    public static final ConfigOption<String> CONNECTION_DEFAULT_SCOPE = ConfigOptions
            .key("connection.default-scope")
            .stringType()
            .noDefaultValue()
            .withDescription("connection.default-scope");

    public static final ConfigOption<String> CONNECTION_SECURITY = ConfigOptions
            .key("connection.security")
            .stringType()
            .noDefaultValue()
            .withDescription("connection.security");

    public static final ConfigOption<String> CONNECTION_SECURITY_AUTH_TYPE = ConfigOptions
            .key("connection.security.auth-type")
            .stringType()
            .noDefaultValue()
            .withDescription("connection.security.auth-type");

    public static final ConfigOption<String> CONNECTION_SECURITY_AUTH_TOKEN = ConfigOptions
            .key("connection.security.auth-token")
            .stringType()
            .noDefaultValue()
            .withDescription("connection.security.auth-token");


    public static final ConfigOption<String> CONNECTION_SECURITY_VALIDATE_HOSTNAME = ConfigOptions
            .key("connection.security.validate-hostname")
            .stringType()
            .noDefaultValue()
            .withDescription("connection.security.validate-hostname");

    public static final ConfigOption<String> CONNECTION_SECURITY_TRUST_STORE = ConfigOptions
            .key("connection.trust-store")
            .stringType()
            .noDefaultValue()
            .withDescription("connection.trust-store");

    public static final ConfigOption<String> SCAN = ConfigOptions
            .key("scan")
            .stringType()
            .noDefaultValue()
            .withDescription("scan");

    public static final ConfigOption<String> SCAN_STREAM_INFO = ConfigOptions
            .key("scan.stream-info")
            .stringType()
            .noDefaultValue()
            .withDescription("scan.stream-info");

    public static final ConfigOption<String> SCAN_SCOPE = ConfigOptions
            .key("scan.scope")
            .stringType()
            .noDefaultValue()
            .withDescription("scan.scope");

    public static final ConfigOption<String> SCAN_STREAM = ConfigOptions
            .key("scan.stream")
            .stringType()
            .noDefaultValue()
            .withDescription("scan.stream");

    public static final ConfigOption<String> SCAN_START_STREAMCUT = ConfigOptions
            .key("scan.start-streamcut")
            .stringType()
            .noDefaultValue()
            .withDescription("scan.start-streamcut");

    public static final ConfigOption<String> SCAN_END_STREAMCUT = ConfigOptions
            .key("scan.end-streamcut")
            .stringType()
            .noDefaultValue()
            .withDescription("scan.end-streamcut");

    public static final ConfigOption<String> SCAN_READER_GROUP = ConfigOptions
            .key("scan.reader-group")
            .stringType()
            .noDefaultValue()
            .withDescription("scan.reader-group");

    public static final ConfigOption<String> SCAN_READER_GROUP_UID = ConfigOptions
            .key("scan.reader-group.uid")
            .stringType()
            .noDefaultValue()
            .withDescription("scan.reader-group.uid");

    public static final ConfigOption<String> SCAN_READER_GROUP_SCOPE = ConfigOptions
            .key("scan.reader-group.scope")
            .stringType()
            .noDefaultValue()
            .withDescription("scan.reader-group.scope");

    public static final ConfigOption<String> SCAN_READER_GROUP_NAME = ConfigOptions
            .key("scan.reader-group.name")
            .stringType()
            .noDefaultValue()
            .withDescription("scan.reader-group.name");

    public static final ConfigOption<String> SCAN_READER_GROUP_REFRESH_INTERVAL = ConfigOptions
            .key("scan.reader-group.refresh-interval")
            .stringType()
            .noDefaultValue()
            .withDescription("scan.reader-group.refresh-interval");

    public static final ConfigOption<String> SCAN_READER_GROUP_EVENT_READ_TIMEOUT_INTERVAL = ConfigOptions
            .key("scan.reader-group.event-read-timeout-interval")
            .stringType()
            .noDefaultValue()
            .withDescription("scan.reader-group.event-read-timeout-interval");

    public static final ConfigOption<String> SCAN_READER_GROUP_CHECKPOINT_INITIATE_TIMEOUT_INTERVAL = ConfigOptions
            .key("scan.reader-group.checkpoint-initiate-timeout-interval")
            .stringType()
            .noDefaultValue()
            .withDescription("scan.reader-group.checkpoint-initiate-timeout-interval");

    public static final ConfigOption<String> SINK = ConfigOptions
            .key("sink")
            .stringType()
            .noDefaultValue()
            .withDescription("sink");

    public static final ConfigOption<String> SINK_SCOPE = ConfigOptions
            .key("sink.scope")
            .stringType()
            .noDefaultValue()
            .withDescription("sink.scope");

    public static final ConfigOption<String> SINK_STREAM = ConfigOptions
            .key("sink.stream")
            .stringType()
            .noDefaultValue()
            .withDescription("sink.stream");

    public static final ConfigOption<String> SINK_MODE = ConfigOptions
            .key("sink.mode")
            .stringType()
            .noDefaultValue()
            .withDescription("sink.mode");

    public static final ConfigOption<String> EXACTLY_ONCE = ConfigOptions
            .key("exactly_once")
            .stringType()
            .noDefaultValue()
            .withDescription("exactly_once");

    public static final ConfigOption<String> ATLEAST_ONCE = ConfigOptions
            .key("atleast_once")
            .stringType()
            .noDefaultValue()
            .withDescription("atleast_once");

    public static final ConfigOption<String> SINK_TXN_LEASE_RENEWAL_INTERVAL = ConfigOptions
            .key("sink.txn-lease-renewal-interval")
            .stringType()
            .noDefaultValue()
            .withDescription("sink.txn-lease-renewal-interval");

    public static final ConfigOption<String> SINK_ENABLE_WATERMARK = ConfigOptions
            .key("sink.enable-watermark")
            .stringType()
            .noDefaultValue()
            .withDescription("sink.enable-watermark");

    public static final ConfigOption<String> SINK_ROUTINGKEY_FIELD_NAME = ConfigOptions
            .key("sink.routingkey-field-name")
            .stringType()
            .noDefaultValue()
            .withDescription("sink.routingkey-field-name");
}
