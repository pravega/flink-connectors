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

import io.pravega.connectors.flink.dynamic.table.PravegaOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.formats.json.JsonOptions;
import org.apache.flink.table.catalog.CommonCatalogOptions;

/** {@link ConfigOption}s for {@link PravegaCatalogFactory}. */
public class PravegaCatalogFactoryOptions {

    public static final String IDENTIFIER = "pravega";

    public static final ConfigOption<String> DEFAULT_DATABASE =
            ConfigOptions.key(CommonCatalogOptions.DEFAULT_DATABASE_KEY).stringType().noDefaultValue()
                    .withDescription("Required default database");

    // required Pravega controller URI
    public static final ConfigOption<String> CONTROLLER_URI = PravegaOptions.CONTROLLER_URI;

    public static final ConfigOption<String> SCHEMA_REGISTRY_URI =
            ConfigOptions.key("schema-registry-uri").stringType().noDefaultValue().withDescription("Required Schema Registry URI");

    public static final ConfigOption<String> SERIALIZATION_FORMAT =
            ConfigOptions.key("serialization.format").stringType().defaultValue("Avro")
                    .withDescription("Optional serialization format for Pravega catalog. Valid enumerations are ['Avro'(default), 'Json']");

    // Json related options
    public static final ConfigOption<Boolean> JSON_FAIL_ON_MISSING_FIELD = JsonOptions.FAIL_ON_MISSING_FIELD;
    public static final ConfigOption<Boolean> JSON_IGNORE_PARSE_ERRORS = JsonOptions.IGNORE_PARSE_ERRORS;
    public static final ConfigOption<String> JSON_TIMESTAMP_FORMAT = JsonOptions.TIMESTAMP_FORMAT;
    public static final ConfigOption<String> JSON_MAP_NULL_KEY_MODE = JsonOptions.MAP_NULL_KEY_MODE;
    public static final ConfigOption<String> JSON_MAP_NULL_KEY_LITERAL = JsonOptions.MAP_NULL_KEY_LITERAL;
    public static final ConfigOption<Boolean> ENCODE_DECIMAL_AS_PLAIN_NUMBER = JsonOptions.ENCODE_DECIMAL_AS_PLAIN_NUMBER;

    private PravegaCatalogFactoryOptions() {}
}
