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
public interface SchemaRegistryRuntimeProvider {

    // Start up this Schema Registry runtime, block the thread until everytime is ready for this runtime.
    void startUp(PravegaRuntimeOperator pravegaRuntimeOperator);

    // Shutdown this Schema Registry runtime.
    void tearDown();

    // Return a Schema Registry operator for operating this Schema Registry runtime.
    SchemaRegistryRuntimeOperator operator();
}
