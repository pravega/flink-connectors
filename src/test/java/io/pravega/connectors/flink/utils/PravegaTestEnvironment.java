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

import io.pravega.connectors.flink.utils.runtime.PravegaContainerProvider;
import io.pravega.connectors.flink.utils.runtime.PravegaRuntime;
import io.pravega.connectors.flink.utils.runtime.PravegaRuntimeOperator;
import org.apache.flink.connector.testframe.TestResource;

/**
 * A test environment for supporting running a Pravega standalone instance before executing tests.
 */
public class PravegaTestEnvironment implements TestResource {

    private final PravegaContainerProvider provider;

    public PravegaTestEnvironment(PravegaRuntime runtime) {
        this.provider = (PravegaContainerProvider) runtime.provider();
    }

    @Override
    public void startUp() {
        provider.startUp();
    }

    @Override
    public void tearDown() {
        provider.tearDown();
    }

    public PravegaRuntimeOperator operator() {
        return provider.operator();
    }
}
