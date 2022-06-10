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

package io.pravega.connectors.flink.utils;

import io.pravega.connectors.flink.utils.runtime.PravegaRuntime;
import io.pravega.connectors.flink.utils.runtime.SchemaRegistryContainerProvider;
import io.pravega.connectors.flink.utils.runtime.SchemaRegistryRuntime;
import io.pravega.connectors.flink.utils.runtime.SchemaRegistryRuntimeOperator;

/**
 * A test environment for supporting running a Schema Registry service before executing tests.
 */
public class SchemaRegistryTestEnvironment extends PravegaTestEnvironment {

    private final SchemaRegistryContainerProvider schemaRegistryProvider;

    public SchemaRegistryTestEnvironment(PravegaRuntime pravegaRuntime, SchemaRegistryRuntime schemaRegistryRuntime) {
        super(pravegaRuntime);
        this.schemaRegistryProvider = (SchemaRegistryContainerProvider) schemaRegistryRuntime.provider();
    }

    @Override
    public void startUp() {
        super.startUp();
        schemaRegistryProvider.startUp(super.operator());
    }

    @Override
    public void tearDown() {
        super.tearDown();
        schemaRegistryProvider.tearDown();
    }

    public SchemaRegistryRuntimeOperator schemaRegistryOperator() {
        return schemaRegistryProvider.operator();
    }
}
