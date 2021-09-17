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

package io.pravega.connectors.flink.table.catalog.pravega.factories;

import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.dynamic.table.FlinkPravegaDynamicTableFactory;
import io.pravega.connectors.flink.dynamic.table.PravegaOptions;
import io.pravega.connectors.flink.dynamic.table.PravegaOptionsUtil;
import io.pravega.connectors.flink.formats.registry.PravegaRegistryFormatFactory;
import io.pravega.connectors.flink.formats.registry.PravegaRegistryOptions;
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

import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Factory for {@link PravegaCatalog}. */
public class PravegaCatalogFactory implements CatalogFactory {
    // the prefix of checkpoint names json related options
    private static final String JSON_PREFIX = "json.";

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
        options.add(PravegaCatalogFactoryOptions.SECURITY_AUTH_TYPE);
        options.add(PravegaCatalogFactoryOptions.SECURITY_AUTH_TOKEN);
        options.add(PravegaCatalogFactoryOptions.SECURITY_VALIDATE_HOSTNAME);
        options.add(PravegaCatalogFactoryOptions.SECURITY_TRUST_STORE);
        options.add(PravegaCatalogFactoryOptions.SERIALIZATION_FORMAT);
        options.add(PravegaCatalogFactoryOptions.JSON_FAIL_ON_MISSING_FIELD);
        options.add(PravegaCatalogFactoryOptions.JSON_IGNORE_PARSE_ERRORS);
        options.add(PravegaCatalogFactoryOptions.JSON_TIMESTAMP_FORMAT);
        options.add(PravegaCatalogFactoryOptions.JSON_MAP_NULL_KEY_MODE);
        options.add(PravegaCatalogFactoryOptions.JSON_MAP_NULL_KEY_LITERAL);
        options.add(PravegaCatalogFactoryOptions.JSON_ENCODE_DECIMAL_AS_PLAIN_NUMBER);
        return options;
    }

    @Override
    public Catalog createCatalog(Context context) {
        final FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        // skip validating the options that have 'json' prefix since ConfigOptions in
        // PravegaCatalogFactoryOptions don't have 'json' prefix.
        // will validate these options later in PravegaRegistryFormatFactory
        helper.validateExcept(JSON_PREFIX);

        // all catalog options
        ReadableConfig configOptions = helper.getOptions();

        Map<String, String> properties = getCatalogOptions(configOptions);
        PravegaConfig pravegaConfig = PravegaOptionsUtil.getPravegaConfig(configOptions)
                .withDefaultScope(configOptions.get(PravegaCatalogFactoryOptions.DEFAULT_DATABASE))
                .withSchemaRegistryURI(URI.create(configOptions.get(PravegaCatalogFactoryOptions.SCHEMA_REGISTRY_URI)));
        return new PravegaCatalog(
                context.getName(),
                configOptions.get(PravegaCatalogFactoryOptions.DEFAULT_DATABASE),
                properties,
                pravegaConfig,
                configOptions.get(PravegaCatalogFactoryOptions.SERIALIZATION_FORMAT));
    }

    private Map<String, String> getCatalogOptions(ReadableConfig configOptions) {
        Map<String, String> properties = new HashMap<>();

        // table options
        properties.put(FactoryUtil.CONNECTOR.key(), FlinkPravegaDynamicTableFactory.IDENTIFIER);
        properties.put(PravegaOptions.CONTROLLER_URI.key(), configOptions.get(PravegaCatalogFactoryOptions.CONTROLLER_URI));
        properties.put(FactoryUtil.FORMAT.key(), PravegaRegistryFormatFactory.IDENTIFIER);
        properties.put(PravegaCatalogFactoryOptions.SECURITY_AUTH_TYPE.key(),
                configOptions.get(PravegaCatalogFactoryOptions.SECURITY_AUTH_TYPE));
        properties.put(PravegaCatalogFactoryOptions.SECURITY_AUTH_TOKEN.key(),
                configOptions.get(PravegaCatalogFactoryOptions.SECURITY_AUTH_TOKEN));
        properties.put(PravegaCatalogFactoryOptions.SECURITY_VALIDATE_HOSTNAME.key(),
                configOptions.get(PravegaCatalogFactoryOptions.SECURITY_VALIDATE_HOSTNAME).toString());
        properties.put(PravegaCatalogFactoryOptions.SECURITY_TRUST_STORE.key(),
                configOptions.get(PravegaCatalogFactoryOptions.SECURITY_TRUST_STORE));

        // Pravega registry options
        properties.put(
                String.format(
                        "%s.%s",
                        PravegaRegistryFormatFactory.IDENTIFIER, PravegaRegistryOptions.URI.key()),
                configOptions.get(PravegaCatalogFactoryOptions.SCHEMA_REGISTRY_URI));
        properties.put(String.format("%s.%s",
                PravegaRegistryFormatFactory.IDENTIFIER, PravegaRegistryOptions.FORMAT.key()),
                configOptions.get(PravegaCatalogFactoryOptions.SERIALIZATION_FORMAT));

        // options that separate "json" prefix and the configuration
        DelegatingConfiguration delegatingConfiguration = new DelegatingConfiguration((Configuration) configOptions, JSON_PREFIX);

        // put json related options into properties
        Map<String, String> jsonProperties = delegatingConfiguration.toMap();
        jsonProperties.forEach((key, value) ->
                properties.put(String.format("%s.%s", PravegaRegistryFormatFactory.IDENTIFIER, key), value));
        return properties;
    }
}
