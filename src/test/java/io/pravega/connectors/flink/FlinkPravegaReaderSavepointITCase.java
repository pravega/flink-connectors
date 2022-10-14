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
import io.pravega.connectors.flink.utils.IntSequenceExactlyOnceValidator;
import io.pravega.connectors.flink.utils.IntegerDeserializationSchema;
import io.pravega.connectors.flink.utils.IntegerSerializer;
import io.pravega.connectors.flink.utils.NotifyingMapper;
import io.pravega.connectors.flink.utils.PravegaTestEnvironment;
import io.pravega.connectors.flink.utils.SuccessException;
import io.pravega.connectors.flink.utils.ThrottledIntegerWriter;
import io.pravega.connectors.flink.utils.runtime.PravegaRuntime;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.client.JobCancellationException;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.TestLogger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link FlinkPravegaReader} focused on savepoint integration.
 */
@Timeout(value = 120)
public class FlinkPravegaReaderSavepointITCase extends TestLogger {

    // Number of events to produce into the test stream.
    private static final int NUM_STREAM_ELEMENTS = 10000;

    private static final int PARALLELISM = 4;

    // ----------------------------------------------------------------------------
    //  setup
    // ----------------------------------------------------------------------------

    private static final PravegaTestEnvironment PRAVEGA = new PravegaTestEnvironment(PravegaRuntime.container());

    // the flink mini cluster
    private static final MiniCluster MINI_CLUSTER = new MiniCluster(
            new MiniClusterConfiguration.Builder()
                    .setNumTaskManagers(1)
                    .setNumSlotsPerTaskManager(PARALLELISM)
                    .build());

    @TempDir
    private Path tmpFolder;

    @BeforeAll
    public static void setup() throws Exception {
        PRAVEGA.startUp();
        MINI_CLUSTER.start();
    }

    @AfterAll
    public static void tearDown() {
        MINI_CLUSTER.closeAsync();
        PRAVEGA.tearDown();
    }

    // ----------------------------------------------------------------------------
    //  tests
    // ----------------------------------------------------------------------------

    @Test
    public void testPravegaWithSavepoint() throws Exception {
        final int sourceParallelism = 4;
        final int numPravegaSegments = 4;
        final int numElements = NUM_STREAM_ELEMENTS;

        // set up the stream
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        PRAVEGA.operator().createTestStream(streamName, numPravegaSegments);

        // we create two independent Flink jobs (that come from the same program)
        final JobGraph program1 = getFlinkJob(sourceParallelism, streamName, numElements);

        try (
                final EventStreamWriter<Integer> eventWriter = PRAVEGA.operator().getWriter(streamName, new IntegerSerializer());

                // create the producer that writes to the stream
                final ThrottledIntegerWriter producer = new ThrottledIntegerWriter(
                        eventWriter,
                        numElements,
                        numElements / 2,  // the latest when the thread must be un-throttled
                        1,                 // the initial sleep time per element
                        false
                )

        ) {
            // the object on which we block while waiting for the checkpoint completion
            final OneShotLatch sync = new OneShotLatch();
            NotifyingMapper.TO_CALL_ON_COMPLETION.set(sync::trigger);

            // launch the Flink program from a separate thread
            final CheckedThread flinkRunner = new CheckedThread() {
                @Override
                public void go() throws Exception {
                    MINI_CLUSTER.submitJob(program1);
                }
            };

            producer.start();
            flinkRunner.start();

            // wait until at least one checkpoint is complete before triggering the safepoints
            sync.await();

            // now that we are comfortably into the program, trigger a savepoint
            String savepointPath = null;

            // since with the short timeouts we configure in these tests, Pravega Checkpoints
            // sometimes don't complete in time, we retry a bit here
            for (int attempt = 1; savepointPath == null && attempt <= 5; attempt++) {
                savepointPath = MINI_CLUSTER.triggerSavepoint(
                        program1.getJobID(),
                        tmpFolder.toFile().getAbsolutePath(),
                        false,
                        SavepointFormatType.CANONICAL).get();
            }

            assertThat(savepointPath).as("Failed to trigger a savepoint").isNotNull();

            // now cancel the job and relaunch a new one
            MINI_CLUSTER.cancelJob(program1.getJobID());

            try {
                // this throws an exception that the job was cancelled
                flinkRunner.sync();
            } catch (JobCancellationException ignored) {
            }

            producer.unthrottle();

            // now, resume with a new program
            final JobGraph program2 = getFlinkJob(sourceParallelism, streamName, numElements);
            program2.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savepointPath, false));

            // if these calls complete without exception, then the test passes
            try {
                MINI_CLUSTER.executeJobBlocking(program2);
            } catch (Exception e) {
                if (!(ExceptionUtils.getRootCause(e) instanceof SuccessException)) {
                    throw e;
                }
            }
        }
    }

    private JobGraph getFlinkJob(
            final int sourceParallelism,
            final String streamName,
            final int numElements) throws IOException {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(sourceParallelism);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 0L));

        // to make the test faster, we use a combination of fast triggering of checkpoints,
        // but some pauses after completed checkpoints
        env.getCheckpointConfig().setCheckpointInterval(100);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000);

        // we currently need this to work around the case where tasks are
        // started too late, a checkpoint was already triggered, and some tasks
        // never see the checkpoint event
        env.getCheckpointConfig().setCheckpointTimeout(5000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(1);

        // checkpoint to files (but aggregate state below 1 MB) and don't to any async checkpoints
        env.getCheckpointConfig().setCheckpointStorage(
                new FileSystemCheckpointStorage(
                       tmpFolder.toFile().toURI(), 1024 * 1024));

        // the Pravega reader
        final FlinkPravegaReader<Integer> pravegaSource = FlinkPravegaReader.<Integer>builder()
                .forStream(streamName)
                .enableMetrics(false)
                .withPravegaConfig(PRAVEGA.operator().getPravegaConfig())
                .withDeserializationSchema(new IntegerDeserializationSchema())
                .uid("my_reader_name")
                .build();

        env
                .addSource(pravegaSource)

                // hook in the notifying mapper
                .map(new NotifyingMapper<>())
                .setParallelism(1)

                // the sink validates that the exactly-once semantics hold
                // it must be non-parallel so that it sees all elements and can trivially
                // check for duplicates
                .addSink(new IntSequenceExactlyOnceValidator(numElements))
                .setParallelism(1);

        return env.getStreamGraph().getJobGraph();
    }
}