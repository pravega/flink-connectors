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

import io.pravega.client.stream.EventStreamWriter;
import io.pravega.connectors.flink.utils.IntegerDeserializationSchema;
import io.pravega.connectors.flink.utils.IntegerSerializer;
import io.pravega.connectors.flink.utils.PravegaTestEnvironment;
import io.pravega.connectors.flink.utils.ThrottledIntegerWriter;
import io.pravega.connectors.flink.utils.runtime.PravegaRuntime;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@Timeout(value = 120)
public class FlinkPravegaInputFormatITCase extends AbstractTestBase {

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
     * Verifies that the input format:
     *  - correctly reads all records in a given set of multiple Pravega streams
     *  - allows multiple executions
     */
    @Test
    public void testBatchInput() throws Exception {
        final int numElements1 = 100;
        final int numElements2 = 300;

        // set up the stream
        final String streamName1 = RandomStringUtils.randomAlphabetic(20);
        final String streamName2 = RandomStringUtils.randomAlphabetic(20);

        final Set<String> streams = new HashSet<>();
        streams.add(streamName1);
        streams.add(streamName2);

        PRAVEGA.operator().createTestStream(streamName1, 3);
        PRAVEGA.operator().createTestStream(streamName2, 5);

        try (
                final EventStreamWriter<Integer> eventWriter1 = PRAVEGA.operator().getWriter(streamName1, new IntegerSerializer());
                final EventStreamWriter<Integer> eventWriter2 = PRAVEGA.operator().getWriter(streamName2, new IntegerSerializer());

                // create the producer that writes to the stream
                final ThrottledIntegerWriter producer1 = new ThrottledIntegerWriter(
                        eventWriter1,
                        numElements1,
                        numElements1 + 1, // no need to block writer for a batch test
                        0,
                        false
                );

                final ThrottledIntegerWriter producer2 = new ThrottledIntegerWriter(
                        eventWriter2,
                        numElements2,
                        numElements2 + 1, // no need to block writer for a batch test
                        0,
                        false
                )
        ) {
            // write batch input
            producer1.start();
            producer2.start();

            producer1.sync();
            producer2.sync();

            final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(3);

            // simple pipeline that reads from Pravega and collects the events
            DataSet<Integer> integers = env.createInput(
                    FlinkPravegaInputFormat.<Integer>builder()
                            .forStream(streamName1)
                            .forStream(streamName2)
                            .withPravegaConfig(PRAVEGA.operator().getPravegaConfig())
                            .withDeserializationSchema(new IntegerDeserializationSchema())
                            .build(),
                    BasicTypeInfo.INT_TYPE_INFO
            );

            // verify that all events were read
            assertThat(integers.collect().size()).isEqualTo(numElements1 + numElements2);

            // this verifies that the input format allows multiple passes
            assertThat(integers.collect().size()).isEqualTo(numElements1 + numElements2);
        }
    }

    /**
     * Verifies that the input format reads all records exactly-once in the presence of job failures.
     */
    @Test
    public void testBatchInputWithFailure() throws Exception {
        final int numElements = 100;

        // set up the stream
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        PRAVEGA.operator().createTestStream(streamName, 3);

        try (
                final EventStreamWriter<Integer> eventWriter = PRAVEGA.operator().getWriter(streamName, new IntegerSerializer());

                // create the producer that writes to the stream
                final ThrottledIntegerWriter producer = new ThrottledIntegerWriter(
                        eventWriter,
                        numElements,
                        numElements + 1, // no need to block writer for a batch test
                        0,
                        false
                )
        ) {
            // write batch input
            producer.start();
            producer.sync();

            final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 1000L));
            env.setParallelism(3);

            // simple pipeline that reads from Pravega and collects the events
            List<Integer> integers = env.createInput(
                    FlinkPravegaInputFormat.<Integer>builder()
                            .forStream(streamName)
                            .withPravegaConfig(PRAVEGA.operator().getPravegaConfig())
                            .withDeserializationSchema(new IntegerDeserializationSchema())
                            .build(),
                    BasicTypeInfo.INT_TYPE_INFO
            ).map(new FailOnceMapper(numElements / 2)).collect();

            // verify that the job did fail, and all events were still read
            assertThat(FailOnceMapper.hasFailed()).isTrue();
            assertThat(integers.size()).isEqualTo(numElements);

            FailOnceMapper.reset();
        }
    }

    private static class FailOnceMapper extends RichMapFunction<Integer, Integer> {

        @SuppressWarnings("checkstyle:StaticVariableName")
        private static boolean failedOnce;

        private final int failCount;

        FailOnceMapper(int failCount) {
            this.failCount = failCount;
        }

        static void reset() {
            failedOnce = false;
        }

        static boolean hasFailed() {
            return failedOnce;
        }

        @Override
        public Integer map(Integer value) throws Exception {
            if (!failedOnce && value.equals(failCount)) {
                failedOnce = true;
                throw new RuntimeException("Artificial failure");
            }

            return value;
        }
    }
}