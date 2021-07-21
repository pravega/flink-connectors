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

import io.pravega.connectors.flink.table.catalog.pravega.PravegaCatalog;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/** Factory for {@link PravegaCatalog}. */
public class PravegaCatalogFactory implements CatalogFactory {

    private static final Logger LOG = LoggerFactory.getLogger(PravegaCatalogFactory.class);

    @Override
    public String factoryIdentifier() {
        return PravegaCatalogFactoryOptions.IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PravegaCatalogFactoryOptions.DEFAULT_DATABASE);
        options.add(PravegaCatalogFactoryOptions.CONTROLLER_URI);
        options.add(PravegaCatalogFactoryOptions.SCHEMA_REGISTRY_URI);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PravegaCatalogFactoryOptions.SERIALIZATION_FORMAT);
        options.add(PravegaCatalogFactoryOptions.JSON_FAIL_ON_MISSING_FIELD);
        options.add(PravegaCatalogFactoryOptions.JSON_IGNORE_PARSE_ERRORS);
        options.add(PravegaCatalogFactoryOptions.JSON_TIMESTAMP_FORMAT);
        options.add(PravegaCatalogFactoryOptions.JSON_MAP_NULL_KEY_MODE);
        options.add(PravegaCatalogFactoryOptions.JSON_MAP_NULL_KEY_LITERAL);
        return options;
    }

    @Override
    public Catalog createCatalog(Context context) {
        final FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        // skip validating the options that have 'json' prefix since ConfigOptions in
        // PravegaCatalogFactoryOptions don't have 'json' prefix.
        // will validate these options later in PravegaRegistryFormatFactory
        helper.validateExcept("json");
        // all catalog options
        ReadableConfig configOptions = helper.getOptions();
        // options that separate "json" prefix and the configuration
        ReadableConfig delegatingConfiguration = new DelegatingConfiguration((Configuration) configOptions, "json");

        return new PravegaCatalog(
                context.getName(),
                configOptions.get(PravegaCatalogFactoryOptions.DEFAULT_DATABASE),
                configOptions.get(PravegaCatalogFactoryOptions.CONTROLLER_URI),
                configOptions.get(PravegaCatalogFactoryOptions.SCHEMA_REGISTRY_URI),
                configOptions.getOptional(PravegaCatalogFactoryOptions.SERIALIZATION_FORMAT).orElse(null),
                delegatingConfiguration.getOptional(PravegaCatalogFactoryOptions.JSON_FAIL_ON_MISSING_FIELD).orElse(null),
                delegatingConfiguration.getOptional(PravegaCatalogFactoryOptions.JSON_IGNORE_PARSE_ERRORS).orElse(null),
                delegatingConfiguration.getOptional(PravegaCatalogFactoryOptions.JSON_TIMESTAMP_FORMAT).orElse(null),
                delegatingConfiguration.getOptional(PravegaCatalogFactoryOptions.JSON_MAP_NULL_KEY_MODE).orElse(null),
                delegatingConfiguration.getOptional(PravegaCatalogFactoryOptions.JSON_MAP_NULL_KEY_LITERAL).orElse(null));
    }
}
