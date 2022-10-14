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
import io.pravega.connectors.flink.utils.runtime.SchemaRegistryRuntime;
import io.pravega.connectors.flink.utils.runtime.SchemaRegistryRuntimeOperator;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * A test environment for supporting running a Pravega standalone instance
 * and a Schema Registry service before executing tests via extending {@link PravegaTestEnvironment}.
 */
public class SchemaRegistryTestEnvironment extends PravegaTestEnvironment {

    private final SchemaRegistryRuntime schemaRegistryRuntime;

    public SchemaRegistryTestEnvironment(PravegaRuntime pravegaRuntime, SchemaRegistryRuntime schemaRegistryRuntime) {
        super(pravegaRuntime);
        this.schemaRegistryRuntime = schemaRegistryRuntime;
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        super.beforeAll(context);
        schemaRegistryRuntime.startUp(super.operator());
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        super.afterAll(context);
        schemaRegistryRuntime.tearDown();
    }

    /** Start up the test resource. */
    @Override
    public void startUp() {
        super.startUp();
        schemaRegistryRuntime.startUp(super.operator());
    }

    /** Tear down the test resource. */
    @Override
    public void tearDown() {
        super.tearDown();
        schemaRegistryRuntime.tearDown();
    }

    /** Get a common supported set of method for operating Schema Registry which is in container. */
    public SchemaRegistryRuntimeOperator schemaRegistryOperator() {
        return schemaRegistryRuntime.operator();
    }
}
