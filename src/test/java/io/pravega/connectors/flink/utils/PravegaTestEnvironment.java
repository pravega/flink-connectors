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
import io.pravega.connectors.flink.utils.runtime.PravegaRuntimeOperator;
import org.apache.flink.connector.testframe.TestResource;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * A test environment for supporting running a Pravega standalone instance before executing tests.
 */
public class PravegaTestEnvironment implements BeforeAllCallback, AfterAllCallback, TestResource {

    private final PravegaRuntime runtime;

    public PravegaTestEnvironment(PravegaRuntime runtime) {
        this.runtime = runtime;
    }

    /** JUnit 5 Extension setup method. */
    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        runtime.startUp();
    }

    /** JUnit 5 Extension shutdown method. */
    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        runtime.tearDown();
    }

    /** Start up the test resource. */
    @Override
    public void startUp() {
        runtime.startUp();
    }

    /** Tear down the test resource. */
    @Override
    public void tearDown() {
        runtime.tearDown();
    }

    /** Get a common supported set of method for operating Pravega which is in container. */
    public PravegaRuntimeOperator operator() {
        return runtime.operator();
    }
}
