/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.connectors.flink;

import io.pravega.client.stream.EventStreamWriter;
import io.pravega.connectors.flink.util.RandomStringUtils;
import io.pravega.connectors.flink.utils.FlinkMiniClusterWithSavepointCommand;
import io.pravega.connectors.flink.utils.IntSequenceExactlyOnceValidator;
import io.pravega.connectors.flink.utils.NotifyingMapper;
import io.pravega.connectors.flink.utils.SetupUtils;
import io.pravega.connectors.flink.utils.SuccessException;
import io.pravega.connectors.flink.utils.ThrottledIntegerWriter;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.client.JobCancellationException;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.serialization.AbstractDeserializationSchema;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;

/**
 * Automated tests for {@link FlinkPravegaReader}.
 */
@Slf4j
public class FlinkPravegaReaderSavepointTest extends TestLogger {

    // Number of events to produce into the test stream.
    private static final int NUM_STREAM_ELEMENTS = 10000;
    
    private static final int PARALLELISM = 4;

    // ----------------------------------------------------------------------------
    //  setup
    // ----------------------------------------------------------------------------

    // Setup utility.
    private static final SetupUtils SETUP_UTILS = new SetupUtils();

    // the flink mini cluster
    private static final FlinkMiniClusterWithSavepointCommand FLINK = 
            FlinkMiniClusterWithSavepointCommand.create(PARALLELISM);

    //Ensure each test completes within 120 seconds.
    @Rule
    public final Timeout globalTimeout = new Timeout(120, TimeUnit.SECONDS);

    @Rule
    public final TemporaryFolder tmpFolder = new TemporaryFolder();

    @BeforeClass
    public static void setup() throws Exception {
        SETUP_UTILS.startAllServices();
        FLINK.start();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        FLINK.stop();
        SETUP_UTILS.stopAllServices();
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
        SETUP_UTILS.createTestStream(streamName, numPravegaSegments);

        // we create two independent Flink jobs (that come from the same program) 
        final JobGraph program1 = getFlinkJob(sourceParallelism, streamName, numElements);

        try (
                final EventStreamWriter<Integer> eventWriter = SETUP_UTILS.getIntegerWriter(streamName);

                // create the producer that writes to the stream
                final ThrottledIntegerWriter producer = new ThrottledIntegerWriter(
                        eventWriter,
                        numElements,
                        numElements / 2,  // the latest when the thread must be un-throttled
                        1                 // the initial sleep time per element
                )

        ) {
            // the object on which we block while waiting for the checkpoint completion
            final OneShotLatch sync = new OneShotLatch();
            NotifyingMapper.TO_CALL_ON_COMPLETION.set( sync::trigger );

            // launch the Flink program from a separate thread
            final CheckedThread flinkRunner = new CheckedThread() {
                @Override
                public void go() throws Exception {
                    FLINK.submitJobAndWait(program1, false);
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
                try {
                    savepointPath = FLINK.triggerSavepoint(program1.getJobID(), tmpFolder.newFolder().toURI());
                } catch (FlinkMiniClusterWithSavepointCommand.SavepointFailedException ignored) {
                }
            }

            assertNotNull("Failed to trigger a savepoint", savepointPath);

            // now cancel the job and relaunch a new one
            FLINK.cancelJob(program1.getJobID());

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
                FLINK.submitJobAndWait(program2, false);
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

        // checkpoint to files (but aggregate state below 1 MB) and don't to any async checkpoints
        env.setStateBackend(new FsStateBackend(tmpFolder.newFolder().toURI(), 1024 * 1024, false));

        // the Pravega reader
        final FlinkPravegaReader<Integer> pravegaSource = new FlinkPravegaReader<>(
                SETUP_UTILS.getControllerUri(),
                SETUP_UTILS.getScope(),
                Collections.singleton(streamName),
                0,
                new IntDeserializer(),
                "my_reader_name");

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

    // ----------------------------------------------------------------------------
    //  utilities
    // ----------------------------------------------------------------------------

    private static class IntDeserializer extends AbstractDeserializationSchema<Integer> {

        @Override
        public Integer deserialize(byte[] message) throws IOException {
            return ByteBuffer.wrap(message).getInt();
        }

        @Override
        public boolean isEndOfStream(Integer nextElement) {
            return false;
        }
    }
}