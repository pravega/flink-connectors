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

import java.util.function.Supplier;

/**
 * An enum class for providing an operable Schema Registry runtime. We now support only one type of runtime, the
 * container.
 */
public enum SchemaRegistryRuntime {

    /**
     * The whole Schema Registry cluster would run in a docker container, provide the full fledged test
     * backend.
     */
    CONTAINER(SchemaRegistryContainerProvider::new);

    private final Supplier<SchemaRegistryRuntimeProvider> provider;

    SchemaRegistryRuntime(Supplier<SchemaRegistryRuntimeProvider> provider) {
        this.provider = provider;
    }

    public SchemaRegistryRuntimeProvider provider() {
        return provider.get();
    }
}
