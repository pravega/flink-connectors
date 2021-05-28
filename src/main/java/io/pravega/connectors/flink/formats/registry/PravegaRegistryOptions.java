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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.table.api.TableException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

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

    public static final ConfigOption<Boolean> FAIL_ON_MISSING_FIELD =
            ConfigOptions.key("fail-on-missing-field")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional flag to specify whether to fail if a field is missing or not, false by default.");

    public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS =
            ConfigOptions.key("ignore-parse-errors")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional flag to skip fields and rows with parse errors instead of failing;\n"
                                    + "fields are set to null in case of errors, false by default.");

    public static final ConfigOption<String> TIMESTAMP_FORMAT =
            ConfigOptions.key("timestamp-format.standard")
                    .stringType()
                    .defaultValue("SQL")
                    .withDescription(
                            "Optional flag to specify timestamp format, SQL by default."
                                    + " Option ISO-8601 will parse input timestamp in \"yyyy-MM-ddTHH:mm:ss.s{precision}\" format and output timestamp in the same format."
                                    + " Option SQL will parse input timestamp in \"yyyy-MM-dd HH:mm:ss.s{precision}\" format and output timestamp in the same format.");

    public static final ConfigOption<String> MAP_NULL_KEY_MODE =
            ConfigOptions.key("map-null-key.mode")
                    .stringType()
                    .defaultValue("FAIL")
                    .withDescription(
                            "Optional flag to control the handling mode when serializing null key for map data, FAIL by default."
                                    + " Option DROP will drop null key entries for map data."
                                    + " Option LITERAL will use 'map-null-key.literal' as key literal.");

    public static final ConfigOption<String> MAP_NULL_KEY_LITERAL =
            ConfigOptions.key("map-null-key.literal")
                    .stringType()
                    .defaultValue("null")
                    .withDescription(
                            "Optional flag to specify string literal for null keys when 'map-null-key.mode' is LITERAL, \"null\" by default.");

    // --------------------------------------------------------------------------------------------
    // Option enumerations
    // --------------------------------------------------------------------------------------------

    public static final String SQL = "SQL";
    public static final String ISO_8601 = "ISO-8601";

    public static final Set<String> TIMESTAMP_FORMAT_ENUM =
            new HashSet<>(Arrays.asList(SQL, ISO_8601));

    // The handling mode of null key for map data
    public static final String JSON_MAP_NULL_KEY_MODE_FAIL = "FAIL";
    public static final String JSON_MAP_NULL_KEY_MODE_DROP = "DROP";
    public static final String JSON_MAP_NULL_KEY_MODE_LITERAL = "LITERAL";

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    public static TimestampFormat getTimestampFormat(ReadableConfig config) {
        String timestampFormat = config.get(TIMESTAMP_FORMAT);
        switch (timestampFormat) {
            case SQL:
                return TimestampFormat.SQL;
            case ISO_8601:
                return TimestampFormat.ISO_8601;
            default:
                throw new TableException(
                        String.format(
                                "Unsupported timestamp format '%s'. Validator should have checked that.",
                                timestampFormat));
        }
    }

    public static MapNullKeyMode getMapNullKeyMode(ReadableConfig config) {
        String mapNullKeyMode = config.get(MAP_NULL_KEY_MODE);
        switch (mapNullKeyMode.toUpperCase()) {
            case JSON_MAP_NULL_KEY_MODE_FAIL:
                return MapNullKeyMode.FAIL;
            case JSON_MAP_NULL_KEY_MODE_DROP:
                return MapNullKeyMode.DROP;
            case JSON_MAP_NULL_KEY_MODE_LITERAL:
                return MapNullKeyMode.LITERAL;
            default:
                throw new TableException(
                        String.format(
                                "Unsupported map null key handling mode '%s'. Validator should have checked that.",
                                mapNullKeyMode));
        }
    }

    // --------------------------------------------------------------------------------------------
    // Inner classes
    // --------------------------------------------------------------------------------------------

    /** Handling mode for map data with null key. */
    public enum MapNullKeyMode {
        FAIL,
        DROP,
        LITERAL
    }
}
