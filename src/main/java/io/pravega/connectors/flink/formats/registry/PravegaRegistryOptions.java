/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.flink.formats.registry;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.formats.json.JsonOptions;

public class PravegaRegistryOptions {

    public static final ConfigOption<String> URL = ConfigOptions
            .key("uri")
            .stringType()
            .noDefaultValue()
            .withDescription("Required schema registry URI");

    public static final ConfigOption<String> NAMESPACE = ConfigOptions
            .key("namespace")
            .stringType()
            .noDefaultValue()
            .withDescription("Required namespace");

    public static final ConfigOption<String> GROUP_ID = ConfigOptions
            .key("group-id")
            .stringType()
            .noDefaultValue()
            .withDescription("Required groupID");

    public static final ConfigOption<String> FORMAT = ConfigOptions
            .key("format")
            .stringType()
            .defaultValue("Avro")
            .withDescription("Optional flag to set serialization format");

    // --------------------------------------------------------------------------------------------
    // Json Options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<Boolean> FAIL_ON_MISSING_FIELD = JsonOptions.FAIL_ON_MISSING_FIELD;
    public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS = JsonOptions.IGNORE_PARSE_ERRORS;
    public static final ConfigOption<String> TIMESTAMP_FORMAT = JsonOptions.TIMESTAMP_FORMAT;
    public static final ConfigOption<String> MAP_NULL_KEY_MODE = JsonOptions.MAP_NULL_KEY_MODE;
    public static final ConfigOption<String> MAP_NULL_KEY_LITERAL = JsonOptions.MAP_NULL_KEY_LITERAL;
}
