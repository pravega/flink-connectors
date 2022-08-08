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

package io.pravega.connectors.flink.utils.runtime;

/**
 * An abstraction for different Schema Registry runtimes. Providing the common methods for SchemaRegistryTestEnvironment.
 */
public interface SchemaRegistryRuntime {

    /**
     * Start up this Schema Registry runtime, block the thread until everytime is ready for this runtime.
     *
     * @param pravegaRuntimeOperator    The Pravega runtime operator.
     */
    void startUp(PravegaRuntimeOperator pravegaRuntimeOperator);

    /** Shutdown this Schema Registry runtime. */
    void tearDown();

    /**
     * Return an operator for operating this Schema Registry runtime. This operator predefined a set of
     * extremely useful methods for Schema Registry. You can easily add new methods in this operator.
     */
    SchemaRegistryRuntimeOperator operator();

    /**
     * Create a Schema Registry instance in docker. We would start a standalone Schema Registry in TestContainers.
     * This runtime is often used in integration and end-to-end tests.
     */
    static SchemaRegistryRuntime container() {
        return new SchemaRegistryContainerRuntime();
    }
}
