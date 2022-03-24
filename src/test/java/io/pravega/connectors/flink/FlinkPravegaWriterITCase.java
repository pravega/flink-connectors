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

import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.TimeWindow;
import io.pravega.connectors.flink.util.FlinkPravegaUtils;
import io.pravega.connectors.flink.utils.FailingMapper;
import io.pravega.connectors.flink.utils.PravegaTestEnvironment;
import io.pravega.connectors.flink.utils.ThrottledIntegerGeneratingSource;
import io.pravega.connectors.flink.utils.runtime.PravegaRuntime;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

/**
 * Integration tests for {@link FlinkPravegaWriter}.
 */
public class FlinkPravegaWriterITCase {

    // Number of events to generate for each of the tests.
    private static final int EVENT_COUNT_PER_SOURCE = 10000;

    // The maximum time we wait for the checker.
    private static final int WAIT_SECONDS = 30;

    private static final PravegaTestEnvironment PRAVEGA = new PravegaTestEnvironment(PravegaRuntime.CONTAINER);

    // Ensure each test completes within 120 seconds.
    @Rule
    public final Timeout globalTimeout = new Timeout(120, TimeUnit.SECONDS);

    @BeforeClass
    public static void setupPravega() throws Exception {
        PRAVEGA.startUp();
    }

    @AfterClass
    public static void tearDownPravega() throws Exception {
        PRAVEGA.tearDown();
    }

    @Test
    public void testEventTimeOrderedWriter() throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.createLocalEnvironment();

        Stream stream = Stream.of(PRAVEGA.operator().getScope(), "testEventTimeOrderedWriter");
        PRAVEGA.operator().createTestStream(stream.getStreamName(), 1);

        DataStreamSource<Integer> dataStream = execEnv
                .addSource(new ThrottledIntegerGeneratingSource(EVENT_COUNT_PER_SOURCE));

        FlinkPravegaWriter<Integer> pravegaSink = FlinkPravegaWriter.<Integer>builder()
                .withPravegaConfig(PRAVEGA.operator().getPravegaConfig())
                .forStream(stream)
                .withSerializationSchema(new IntSerializer())
                .withEventRouter(event -> "fixedkey")
                .build();

        FlinkPravegaUtils.writeToPravegaInEventTimeOrder(dataStream, pravegaSink, 1);
        Assert.assertNotNull(execEnv.getExecutionPlan());
    }

    /**
     * Tests the {@link FlinkPravegaWriter} in {@code AT_LEAST_ONCE} mode.
     */
    @Test
    public void testAtLeastOnceWriter() throws Exception {
        final String streamName = RandomStringUtils.randomAlphabetic(20);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment()
                .setParallelism(1)
                .enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

        FlinkPravegaWriter<Integer> sink = FlinkPravegaWriter.<Integer>builder()
                .forStream(streamName)
                .withPravegaConfig(PRAVEGA.operator().getPravegaConfig())
                .withSerializationSchema(new IntSerializer())
                .withEventRouter(event -> "fixedkey")
                .withWriterMode(PravegaWriterMode.ATLEAST_ONCE)
                .build();

        env
                .addSource(new ThrottledIntegerGeneratingSource(EVENT_COUNT_PER_SOURCE))
                .map(new FailingMapper<>(EVENT_COUNT_PER_SOURCE / 2))
                .addSink(sink).setParallelism(2);

        writeAndCheckData(streamName, env, true);
    }

    /**
     * Tests the {@link FlinkPravegaWriter} in {@code AT_LEAST_ONCE} mode with watermarking.
     */
    @Test
    public void testAtLeastOnceWriterWithWatermark() throws Exception {
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        PRAVEGA.operator().createTestStream(streamName, 1);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment()
                .setParallelism(1)
                .enableCheckpointing(1000, CheckpointingMode.AT_LEAST_ONCE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
        env.getConfig().setAutoWatermarkInterval(50);

        FlinkPravegaWriter<Integer> sink = FlinkPravegaWriter.<Integer>builder()
                .forStream(streamName)
                .withPravegaConfig(PRAVEGA.operator().getPravegaConfig())
                .withSerializationSchema(new IntSerializer())
                .withWriterMode(PravegaWriterMode.ATLEAST_ONCE)
                .enableWatermark(true)
                .build();

        env
                .addSource(new ThrottledIntegerGeneratingSource(EVENT_COUNT_PER_SOURCE))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Integer>forMonotonousTimestamps()
                        .withTimestampAssigner((event, timestamp) -> event))
                .addSink(sink).setParallelism(2);

        writeAndCheckData(streamName, env, true);
        checkWatermark(streamName);
    }

    /**
     * Tests the {@link FlinkPravegaWriter} in {@code EXACTLY_ONCE} mode.
     */
    @Test
    public void testExactlyOnceWriter() throws Exception {
        final String streamName = RandomStringUtils.randomAlphabetic(20);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setParallelism(1)
                .enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0L));

        final FlinkPravegaWriter<Integer> sink = FlinkPravegaWriter.<Integer>builder()
                .forStream(streamName)
                .withPravegaConfig(PRAVEGA.operator().getPravegaConfig())
                .withSerializationSchema(new IntSerializer())
                .withEventRouter(event -> "fixedkey")
                .withWriterMode(PravegaWriterMode.EXACTLY_ONCE)
                .withTxnLeaseRenewalPeriod(Time.seconds(30))
                .build();

        env
                .addSource(new ThrottledIntegerGeneratingSource(EVENT_COUNT_PER_SOURCE))
                .map(new FailingMapper<>(EVENT_COUNT_PER_SOURCE / 2))
                .addSink(sink).setParallelism(2);

        writeAndCheckData(streamName, env, false);
    }

    /**
     * Tests the {@link FlinkPravegaWriter} in {@code EXACTLY_ONCE} mode with unaligned checkpoint.
     */
    @Test
    public void testExactlyOnceWithUnalignedCheckpointWriter() throws Exception {
        final String streamName = RandomStringUtils.randomAlphabetic(20);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setParallelism(1)
                .enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0L));

        final FlinkPravegaWriter<Integer> sink = FlinkPravegaWriter.<Integer>builder()
                .forStream(streamName)
                .withPravegaConfig(PRAVEGA.operator().getPravegaConfig())
                .withSerializationSchema(new IntSerializer())
                .withEventRouter(event -> "fixedkey")
                .withWriterMode(PravegaWriterMode.EXACTLY_ONCE)
                .withTxnLeaseRenewalPeriod(Time.seconds(30))
                .build();

        env
                .addSource(new ThrottledIntegerGeneratingSource(EVENT_COUNT_PER_SOURCE))
                .map(new FailingMapper<>(EVENT_COUNT_PER_SOURCE / 2))
                .addSink(sink).setParallelism(2);

        writeAndCheckData(streamName, env, false);
    }

    /**
     * Tests the {@link FlinkPravegaWriter} in {@code EXACTLY_ONCE} mode with watermarking.
     */
    @Test
    public void testExactlyOnceWriterWithWatermark() throws Exception {
        final String streamName = RandomStringUtils.randomAlphabetic(20);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setParallelism(1)
                .enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.getConfig().setAutoWatermarkInterval(100);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0L));

        final FlinkPravegaWriter<Integer> sink = FlinkPravegaWriter.<Integer>builder()
                .forStream(streamName)
                .withPravegaConfig(PRAVEGA.operator().getPravegaConfig())
                .withSerializationSchema(new IntSerializer())
                .withWriterMode(PravegaWriterMode.EXACTLY_ONCE)
                .withTxnLeaseRenewalPeriod(Time.seconds(30))
                .enableWatermark(true)
                .build();

        env
                .addSource(new ThrottledIntegerGeneratingSource(EVENT_COUNT_PER_SOURCE).withWatermarks(20))
                .map(new FailingMapper<>(EVENT_COUNT_PER_SOURCE / 2))
                .addSink(sink).setParallelism(2);

        writeAndCheckData(streamName, env, false);
        checkWatermark(streamName);
    }

    // ----------------------------------------------------------------------------

    private static class IntSerializer implements SerializationSchema<Integer> {

        @Override
        public byte[] serialize(Integer integer) {
            return ByteBuffer.allocate(4).putInt(0, integer).array();
        }
    }

    /**
     * Write some events and check the result concurrently.
     *
     * @param streamName         The test Pravega stream name.
     * @param env                The Flink environment to be executed, with the job graph configured.
     * @param allowDuplicate     Check data in AT_LEAST_ONCE or EXACTLY_ONCE mode.
     * @throws Exception on any errors.
     */
    private void writeAndCheckData(String streamName,
                           StreamExecutionEnvironment env,
                           boolean allowDuplicate) throws Exception {
        PRAVEGA.operator().createTestStream(streamName, 4);

        // A synchronization aid that allows the program to wait until
        // both writer and checker complete their tasks.
        CountDownLatch latch = new CountDownLatch(2);

        Runnable writeTask = () -> {
            try {
                env.execute();
            } catch (Exception e) {
                Assert.fail("Error while writing to Pravega");
            } finally {
                latch.countDown();
            }
        };

        Runnable checkTask = () -> {
            // Validate the data.
            // 1. Check if all the events are written to the Pravega stream
            // 2. (Optional, controlled by allowDuplicate) Check if there is a duplication
            // 3. Check there is no more events
            try (EventStreamReader<Integer> reader = PRAVEGA.operator().getIntegerReader(streamName)) {
                final BitSet checker = new BitSet();

                while (checker.nextClearBit(1) <= EVENT_COUNT_PER_SOURCE) {
                    final EventRead<Integer> eventRead = reader.readNextEvent(1000);
                    final Integer event = eventRead.getEvent();

                    if (event != null) {
                        if (!allowDuplicate) {
                            assertFalse("found a duplicate", checker.get(event));
                        }
                        checker.set(event);
                    }
                }

                if (!allowDuplicate) {
                    // No more events should be there
                    assertNull("too many elements written", reader.readNextEvent(1000).getEvent());
                }

                // Notify that the checker is complete
                latch.countDown();
            }
        };

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.execute(writeTask);
        executorService.execute(checkTask);

        boolean wait = latch.await(WAIT_SECONDS, TimeUnit.SECONDS);
        if (!wait) {
            Assert.fail("Read/Write operations taking more time to complete");
        }
        executorService.shutdown();
        if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
            executorService.shutdownNow();
        }
    }

    /**
     * Check the watermark generated by the Pravega.
     *
     * @param streamName         The Pravega stream name.
     * @throws InterruptedException on interruption.
     */
    private void checkWatermark(String streamName) throws InterruptedException, UnknownHostException {
        // Wait 11 seconds for the Pravega controller to generate TimeWindow
        Thread.sleep(11000);

        EventStreamReader<Integer> consumer = PRAVEGA.operator().getIntegerReader(streamName);
        consumer.readNextEvent(1000);
        TimeWindow timeWindow = consumer.getCurrentTimeWindow(PRAVEGA.operator().getStream(streamName));

        // Assert the TimeWindow proceeds
        Assert.assertNotNull(timeWindow.getUpperTimeBound());
        Assert.assertTrue(timeWindow.getUpperTimeBound() > 0);
    }
}
