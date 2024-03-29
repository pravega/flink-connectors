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

package io.pravega.connectors.flink;

import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.utils.IntegerDeserializationSchema;
import io.pravega.connectors.flink.utils.IntegerSerializationSchema;
import io.pravega.connectors.flink.utils.PravegaTestEnvironment;
import io.pravega.connectors.flink.utils.runtime.PravegaRuntime;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

@Timeout(value = 120)
public class FlinkPravegaOutputFormatITCase extends AbstractTestBase {

    private static final PravegaTestEnvironment PRAVEGA = new PravegaTestEnvironment(PravegaRuntime.container());

    @BeforeAll
    public static void setupPravega() throws Exception {
        PRAVEGA.startUp();
    }

    @AfterAll
    public static void tearDownPravega() throws Exception {
        PRAVEGA.tearDown();
    }

    // ------------------------------------------------------------------------

    /**
     * Verifies the following using DataSet API:
     *  - writes data into Pravega using {@link FlinkPravegaOutputFormat}.
     *  - reads data from Pravega using {@link FlinkPravegaInputFormat}.
     */
    @Test
    public void testPravegaOutputFormat() throws Exception {

        Stream stream = Stream.of(PRAVEGA.operator().getScope(), "outputFormatDataSet");
        PRAVEGA.operator().createTestStream(stream.getStreamName(), 1);

        PravegaConfig pravegaConfig = PRAVEGA.operator().getPravegaConfig();

        FlinkPravegaOutputFormat<Integer> flinkPravegaOutputFormat = FlinkPravegaOutputFormat.<Integer>builder()
                .withEventRouter(router -> "fixedKey")
                .withSerializationSchema(new IntegerSerializationSchema())
                .withPravegaConfig(pravegaConfig)
                .forStream(stream)
                .build();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Collection<Integer> inputData = Arrays.asList(10, 20);
        env.fromCollection(inputData)
                .output(flinkPravegaOutputFormat);
        env.execute("write");

        DataSet<Integer> integers = env.createInput(
                FlinkPravegaInputFormat.<Integer>builder()
                        .forStream(stream)
                        .withPravegaConfig(PRAVEGA.operator().getPravegaConfig())
                        .withDeserializationSchema(new IntegerDeserializationSchema())
                        .build(),
                BasicTypeInfo.INT_TYPE_INFO
        );

        // verify that all events were read
        assertThat(integers.collect().size()).isEqualTo(2);
    }


}
