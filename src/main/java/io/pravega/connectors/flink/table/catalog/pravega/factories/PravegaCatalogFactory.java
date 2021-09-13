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

import io.pravega.connectors.flink.table.catalog.pravega.PravegaCatalog;
import io.pravega.connectors.flink.table.catalog.pravega.descriptors.PravegaCatalogValidator;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.CatalogFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
        final DescriptorProperties dp = getValidateProperties(properties);

        final Optional<String> serializationFormat =
                dp.getOptionalString(CATALOG_SERIALIZATION_FORMAT);
        final Optional<String> failOnMissingField =
                dp.getOptionalString(CATALOG_JSON_FAIL_ON_MISSING_FIELD);
        final Optional<String> ignoreParseErrors =
                dp.getOptionalString(CATALOG_JSON_IGNORE_PARSE_ERRORS);
        final Optional<String> timestampFormat =
                dp.getOptionalString(CATALOG_JSON_TIMESTAMP_FORMAT);
        final Optional<String> mapNullKeyMode =
                dp.getOptionalString(CATALOG_JSON_MAP_NULL_KEY_MODE);
        final Optional<String> mapNullKeyLiteral =
                dp.getOptionalString(CATALOG_JSON_MAP_NULL_KEY_LITERAL);

        return new PravegaCatalog(
                name,
                dp.getString(CATALOG_DEFAULT_DATABASE),
                dp.getString(CATALOG_CONTROLLER_URI),
                dp.getString(CATALOG_SCHEMA_REGISTRY_URI),
                serializationFormat.orElse(null),
                failOnMissingField.orElse(null),
                ignoreParseErrors.orElse(null),
                timestampFormat.orElse(null),
                mapNullKeyMode.orElse(null),
                mapNullKeyLiteral.orElse(null));
    }

    private DescriptorProperties getValidateProperties(Map<String, String> properties) {
        final DescriptorProperties dp = new DescriptorProperties();
        dp.putProperties(properties);

        new PravegaCatalogValidator().validate(dp);
        return dp;
    }
}
