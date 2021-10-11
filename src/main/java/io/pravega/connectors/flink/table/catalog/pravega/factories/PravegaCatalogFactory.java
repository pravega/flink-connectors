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
import io.pravega.connectors.flink.table.catalog.pravega.descriptors.PravegaCatalogValidator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.JsonOptions;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.pravega.connectors.flink.table.catalog.pravega.descriptors.PravegaCatalogValidator.CATALOG_CONTROLLER_URI;
import static io.pravega.connectors.flink.table.catalog.pravega.descriptors.PravegaCatalogValidator.CATALOG_JSON_FAIL_ON_MISSING_FIELD;
import static io.pravega.connectors.flink.table.catalog.pravega.descriptors.PravegaCatalogValidator.CATALOG_JSON_IGNORE_PARSE_ERRORS;
import static io.pravega.connectors.flink.table.catalog.pravega.descriptors.PravegaCatalogValidator.CATALOG_JSON_MAP_NULL_KEY_LITERAL;
import static io.pravega.connectors.flink.table.catalog.pravega.descriptors.PravegaCatalogValidator.CATALOG_JSON_MAP_NULL_KEY_MODE;
import static io.pravega.connectors.flink.table.catalog.pravega.descriptors.PravegaCatalogValidator.CATALOG_JSON_TIMESTAMP_FORMAT;
import static io.pravega.connectors.flink.table.catalog.pravega.descriptors.PravegaCatalogValidator.CATALOG_PROPERTY_VERSION;
import static io.pravega.connectors.flink.table.catalog.pravega.descriptors.PravegaCatalogValidator.CATALOG_SCHEMA_REGISTRY_URI;
import static io.pravega.connectors.flink.table.catalog.pravega.descriptors.PravegaCatalogValidator.CATALOG_SERIALIZATION_FORMAT;
import static io.pravega.connectors.flink.table.catalog.pravega.descriptors.PravegaCatalogValidator.CATALOG_TYPE;
import static io.pravega.connectors.flink.table.catalog.pravega.descriptors.PravegaCatalogValidator.CATALOG_TYPE_VALUE_PRAVEGA;
import static io.pravega.connectors.flink.table.catalog.pravega.descriptors.PravegaCatalogValidator.SECURITY_AUTH_TOKEN;
import static io.pravega.connectors.flink.table.catalog.pravega.descriptors.PravegaCatalogValidator.SECURITY_AUTH_TYPE;
import static io.pravega.connectors.flink.table.catalog.pravega.descriptors.PravegaCatalogValidator.SECURITY_TRUST_STORE;
import static io.pravega.connectors.flink.table.catalog.pravega.descriptors.PravegaCatalogValidator.SECURITY_VALIDATE_HOSTNAME;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_DEFAULT_DATABASE;

public class PravegaCatalogFactory implements CatalogFactory {

    @Override
    public Map<String, String> requiredContext() {
        HashMap<String, String> context = new HashMap<>();
        context.put(CATALOG_TYPE, CATALOG_TYPE_VALUE_PRAVEGA);
        context.put(CATALOG_PROPERTY_VERSION, "1"); // backwards compatibility
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> props = new ArrayList<>();

        props.add(CATALOG_DEFAULT_DATABASE);

        props.add(CATALOG_CONTROLLER_URI);
        props.add(CATALOG_SCHEMA_REGISTRY_URI);
        props.add(SECURITY_AUTH_TYPE);
        props.add(SECURITY_AUTH_TOKEN);
        props.add(SECURITY_VALIDATE_HOSTNAME);
        props.add(SECURITY_TRUST_STORE);

        props.add(CATALOG_SERIALIZATION_FORMAT);
        props.add(CATALOG_JSON_FAIL_ON_MISSING_FIELD);
        props.add(CATALOG_JSON_IGNORE_PARSE_ERRORS);
        props.add(CATALOG_JSON_TIMESTAMP_FORMAT);
        props.add(CATALOG_JSON_MAP_NULL_KEY_MODE);
        props.add(CATALOG_JSON_MAP_NULL_KEY_LITERAL);
        return props;
    }

    @Override
    public Catalog createCatalog(String name, Map<String, String> properties) {
        validateProperties(properties);

        Configuration configOptions = Configuration.fromMap(properties);
        PravegaConfig pravegaConfig = PravegaOptionsUtil.getPravegaConfig(configOptions)
                .withDefaultScope(properties.get(CATALOG_DEFAULT_DATABASE))
                .withSchemaRegistryURI(URI.create(properties.get(CATALOG_SCHEMA_REGISTRY_URI)));
        Map<String, String> catalogProperties = getCatalogOptions(properties);
        return new PravegaCatalog(
                name,
                properties.get(CATALOG_DEFAULT_DATABASE),
                catalogProperties,
                pravegaConfig,
                properties.getOrDefault(CATALOG_SERIALIZATION_FORMAT, null));
    }

    private void validateProperties(Map<String, String> properties) {
        final DescriptorProperties dp = new DescriptorProperties();
        dp.putProperties(properties);

        new PravegaCatalogValidator().validate(dp);
    }

    private Map<String, String> getCatalogOptions(Map<String, String> properties) {
        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put(FactoryUtil.CONNECTOR.key(), FlinkPravegaDynamicTableFactory.IDENTIFIER);
        catalogProperties.put(PravegaOptions.CONTROLLER_URI.key(), properties.get(CATALOG_CONTROLLER_URI));
        catalogProperties.put(FactoryUtil.FORMAT.key(), PravegaRegistryFormatFactory.IDENTIFIER);
        catalogProperties.put(
                String.format(
                        "%s.%s",
                        PravegaRegistryFormatFactory.IDENTIFIER, PravegaRegistryOptions.URI.key()),
                properties.get(CATALOG_SCHEMA_REGISTRY_URI));

        // put security options into properties
        copyOptions(SECURITY_AUTH_TYPE, properties, catalogProperties);
        copyOptions(SECURITY_AUTH_TOKEN, properties, catalogProperties);
        copyOptions(SECURITY_VALIDATE_HOSTNAME, properties, catalogProperties);
        copyOptions(SECURITY_TRUST_STORE, properties, catalogProperties);

        // put json related options into properties
        if (properties.containsKey(CATALOG_JSON_FAIL_ON_MISSING_FIELD)) {
            catalogProperties.put(String.format("%s.%s",
                            PravegaRegistryFormatFactory.IDENTIFIER, JsonOptions.FAIL_ON_MISSING_FIELD.key()),
                    properties.get(CATALOG_JSON_FAIL_ON_MISSING_FIELD));
        }
        if (properties.containsKey(CATALOG_JSON_IGNORE_PARSE_ERRORS)) {
            catalogProperties.put(String.format("%s.%s",
                            PravegaRegistryFormatFactory.IDENTIFIER, JsonOptions.IGNORE_PARSE_ERRORS.key()),
                    properties.get(CATALOG_JSON_IGNORE_PARSE_ERRORS));
        }
        if (properties.containsKey(CATALOG_JSON_TIMESTAMP_FORMAT)) {
            catalogProperties.put(String.format("%s.%s",
                            PravegaRegistryFormatFactory.IDENTIFIER, JsonOptions.TIMESTAMP_FORMAT.key()),
                    properties.get(CATALOG_JSON_TIMESTAMP_FORMAT));
        }
        if (properties.containsKey(CATALOG_JSON_MAP_NULL_KEY_MODE)) {
            catalogProperties.put(String.format("%s.%s",
                            PravegaRegistryFormatFactory.IDENTIFIER, JsonOptions.MAP_NULL_KEY_MODE.key()),
                    properties.get(CATALOG_JSON_MAP_NULL_KEY_MODE));
        }
        if (properties.containsKey(CATALOG_JSON_MAP_NULL_KEY_LITERAL)) {
            catalogProperties.put(String.format("%s.%s",
                            PravegaRegistryFormatFactory.IDENTIFIER, JsonOptions.MAP_NULL_KEY_LITERAL.key()),
                    properties.get(CATALOG_JSON_MAP_NULL_KEY_LITERAL));
        }
        return catalogProperties;
    }

    private void copyOptions(String key, Map<String, String> sourceMap, Map<String, String> targetMap) {
        if (sourceMap.containsKey(key)) {
            targetMap.put(key, sourceMap.get(key));
        }
    }
}
