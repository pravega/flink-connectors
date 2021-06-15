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

import org.apache.flink.table.descriptors.CatalogDescriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;

import java.util.Map;

import static io.pravega.connectors.flink.table.catalog.pravega.descriptors.PravegaCatalogValidator.CATALOG_CONTROLLER_URI;
import static io.pravega.connectors.flink.table.catalog.pravega.descriptors.PravegaCatalogValidator.CATALOG_JSON_FAIL_ON_MISSING_FIELD;
import static io.pravega.connectors.flink.table.catalog.pravega.descriptors.PravegaCatalogValidator.CATALOG_JSON_IGNORE_PARSE_ERRORS;
import static io.pravega.connectors.flink.table.catalog.pravega.descriptors.PravegaCatalogValidator.CATALOG_JSON_MAP_NULL_KEY_LITERAL;
import static io.pravega.connectors.flink.table.catalog.pravega.descriptors.PravegaCatalogValidator.CATALOG_JSON_MAP_NULL_KEY_MODE;
import static io.pravega.connectors.flink.table.catalog.pravega.descriptors.PravegaCatalogValidator.CATALOG_JSON_TIMESTAMP_FORMAT;
import static io.pravega.connectors.flink.table.catalog.pravega.descriptors.PravegaCatalogValidator.CATALOG_SCHEMA_REGISTRY_URI;
import static io.pravega.connectors.flink.table.catalog.pravega.descriptors.PravegaCatalogValidator.CATALOG_SERIALIZATION_FORMAT;
import static io.pravega.connectors.flink.table.catalog.pravega.descriptors.PravegaCatalogValidator.CATALOG_TYPE_VALUE_PRAVEGA;

public class PravegaCatalogDescriptor extends CatalogDescriptor {

    private final String controllerUri;
    private final String schemaRegistryUri;

    private final String serializationFormat;
    private final String failOnMissingField;
    private final String ignoreParseErrors;
    private final String timestampFormat;
    private final String mapNullKeyMode;
    private final String mapNullKeyLiteral;

    public PravegaCatalogDescriptor(String controllerUri, String schemaRegistryUri, String defaultDatabase,
                                    String serializationFormat, String failOnMissingField, String ignoreParseErrors,
                                    String timestampFormat, String mapNullKeyMode, String mapNullKeyLiteral) {
        super(CATALOG_TYPE_VALUE_PRAVEGA, 1, defaultDatabase);

        this.controllerUri = controllerUri;
        this.schemaRegistryUri = schemaRegistryUri;

        this.serializationFormat = serializationFormat;
        this.failOnMissingField = failOnMissingField;
        this.ignoreParseErrors = ignoreParseErrors;
        this.timestampFormat = timestampFormat;
        this.mapNullKeyMode = mapNullKeyMode;
        this.mapNullKeyLiteral = mapNullKeyLiteral;
    }

    @Override
    protected Map<String, String> toCatalogProperties() {
        final DescriptorProperties properties = new DescriptorProperties();

        properties.putString(CATALOG_CONTROLLER_URI, controllerUri);
        properties.putString(CATALOG_SCHEMA_REGISTRY_URI, schemaRegistryUri);

        if (serializationFormat != null) {
            properties.putString(CATALOG_SERIALIZATION_FORMAT, serializationFormat);
        }
        if (failOnMissingField != null) {
            properties.putString(CATALOG_JSON_FAIL_ON_MISSING_FIELD, failOnMissingField);
        }
        if (ignoreParseErrors != null) {
            properties.putString(CATALOG_JSON_IGNORE_PARSE_ERRORS, ignoreParseErrors);
        }
        if (timestampFormat != null) {
            properties.putString(CATALOG_JSON_TIMESTAMP_FORMAT, timestampFormat);
        }
        if (mapNullKeyMode != null) {
            properties.putString(CATALOG_JSON_MAP_NULL_KEY_MODE, mapNullKeyMode);
        }
        if (mapNullKeyLiteral != null) {
            properties.putString(CATALOG_JSON_MAP_NULL_KEY_LITERAL, mapNullKeyLiteral);
        }

        return properties.asMap();
    }
}

