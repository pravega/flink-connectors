/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.flink.table.catalog.pravega.factories;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.CommonCatalogOptions;

/** {@link ConfigOption}s for {@link PravegaCatalogFactory}. */
public class PravegaCatalogFactoryOptions {

    public static final String IDENTIFIER = "pravega";

    public static final ConfigOption<String> DEFAULT_DATABASE =
            ConfigOptions.key(CommonCatalogOptions.DEFAULT_DATABASE_KEY).stringType().noDefaultValue();

    public static final ConfigOption<String> CONTROLLER_URI =
            ConfigOptions.key("controller-uri").stringType().noDefaultValue();

    public static final ConfigOption<String> SCHEMA_REGISTRY_URI =
            ConfigOptions.key("schema-registry-uri").stringType().noDefaultValue();

    public static final ConfigOption<String> SERIALIZATION_FORMAT =
            ConfigOptions.key("serialization.format").stringType().noDefaultValue();

    public static final ConfigOption<String> JSON_FAIL_ON_MISSING_FIELD =
            ConfigOptions.key("json.fail-on-missing-field").stringType().noDefaultValue();

    public static final ConfigOption<String> JSON_IGNORE_PARSE_ERRORS =
            ConfigOptions.key("json.ignore-parse-errors").stringType().noDefaultValue();

    public static final ConfigOption<String> JSON_TIMESTAMP_FORMAT =
            ConfigOptions.key("json.timestamp-format.standard").stringType().noDefaultValue();

    public static final ConfigOption<String> JSON_MAP_NULL_KEY_MODE =
            ConfigOptions.key("json.map-null-key.mode").stringType().noDefaultValue();

    public static final ConfigOption<String> JSON_MAP_NULL_KEY_LITERAL =
            ConfigOptions.key("json.map-null-key.literal").stringType().noDefaultValue();

    private PravegaCatalogFactoryOptions() {}
}
