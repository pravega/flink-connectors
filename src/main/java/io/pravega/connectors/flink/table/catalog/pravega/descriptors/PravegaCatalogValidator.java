/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.flink.table.catalog.pravega.descriptors;

import org.apache.flink.table.descriptors.CatalogDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;

public class PravegaCatalogValidator extends CatalogDescriptorValidator {
    public static final String CATALOG_TYPE_VALUE_PRAVEGA = "pravega";

    public static final String CATALOG_CONTROLLER_URI = "controller-uri";
    public static final String CATALOG_SCHEMA_REGISTRY_URI = "schema-registry-uri";

    public static final String CATALOG_SERIALIZATION_FORMAT = "serialization.format";
    public static final String CATALOG_JSON_FAIL_ON_MISSING_FIELD = "json.fail-on-missing-field";
    public static final String CATALOG_JSON_IGNORE_PARSE_ERRORS = "json.ignore-parse-errors";
    public static final String CATALOG_JSON_TIMESTAMP_FORMAT = "json.timestamp-format.standard";
    public static final String CATALOG_JSON_MAP_NULL_KEY_MODE = "json.map-null-key.mode";
    public static final String CATALOG_JSON_MAP_NULL_KEY_LITERAL = "json.map-null-key.literal";

    @Override
    public void validate(DescriptorProperties properties) {
        super.validate(properties);

        properties.validateValue(CATALOG_TYPE, CATALOG_TYPE_VALUE_PRAVEGA, false);
        properties.validateString(CATALOG_CONTROLLER_URI, false, 1);
        properties.validateString(CATALOG_SCHEMA_REGISTRY_URI, false, 1);
        properties.validateString(CATALOG_DEFAULT_DATABASE, false, 1);

        properties.validateString(CATALOG_SERIALIZATION_FORMAT, true, 1);
        properties.validateString(CATALOG_JSON_FAIL_ON_MISSING_FIELD, true, 1);
        properties.validateString(CATALOG_JSON_IGNORE_PARSE_ERRORS, true, 1);
        properties.validateString(CATALOG_JSON_TIMESTAMP_FORMAT, true, 1);
        properties.validateString(CATALOG_JSON_MAP_NULL_KEY_MODE, true, 1);
        properties.validateString(CATALOG_JSON_MAP_NULL_KEY_LITERAL, true, 1);
    }
}
