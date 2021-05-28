/**
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.connectors.flink;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.utils.IntSequenceExactlyOnceValidator;
import io.pravega.connectors.flink.utils.IntegerDeserializationSchema;
import io.pravega.connectors.flink.utils.IntegerSerializer;
import io.pravega.connectors.flink.utils.IntentionalException;
import io.pravega.connectors.flink.utils.SetupUtils;
import io.pravega.connectors.flink.utils.SuccessException;
import io.pravega.connectors.flink.utils.ThrottledIntegerWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test case that validates the following.
 * 1. Start Pravega cluster and ingest some data.
 * 2. Start a job that reads from Pravega stream.
 * 3. Simulate an error before the entire data is being read completely.
 * 5. Now Pravega readers will be at some position but Flink is not aware of the state.
 * 6. Flink restart strategy should kick in and the reader hook should reinitialize the state of the readers to beginning position
 * 7. Validate and make sure that we are not missing any events.
 */
@Slf4j
public class FlinkPravegaReaderRGStateITCase extends AbstractTestBase {

    // Setup utility.
    protected static final SetupUtils SETUP_UTILS = new SetupUtils();

    // Number of events to produce into the test stream.
    private static final int NUM_STREAM_ELEMENTS = 100;

    @Rule
    public final Timeout globalTimeout = new Timeout(180, TimeUnit.SECONDS);

    @BeforeClass
    public static void setupPravega() throws Exception {
        SETUP_UTILS.startAllServices();
    }

    @AfterClass
    public static void tearDownPravega() throws Exception {
        SETUP_UTILS.stopAllServices();
    }

    @Test
    public void testReaderState() throws Exception {

        int numPravegaSegments = 3;

        final String streamName = RandomStringUtils.randomAlphabetic(20);
        final String sideStream = RandomStringUtils.randomAlphabetic(20);

        SETUP_UTILS.createTestStream(streamName, numPravegaSegments);
        SETUP_UTILS.createTestStream(sideStream, 1);

        final int threshold = NUM_STREAM_ELEMENTS / 20;
        try (final EventStreamWriter<Integer> writer = SETUP_UTILS.getIntegerWriter(sideStream)) {
            for (int i = 1; i <= threshold; i++) {
                writer.writeEvent(1).get();
            }
            writer.writeEvent(2).get();
        }

        try (final EventStreamWriter<Integer> eventWriter = SETUP_UTILS.getIntegerWriter(streamName);
             // create the producer that writes to the stream
             final ThrottledIntegerWriter producer = new ThrottledIntegerWriter(
                     eventWriter,
                     NUM_STREAM_ELEMENTS,
                     NUM_STREAM_ELEMENTS / 2,
                     1,
                     false
             )
        ) {
            producer.start();
            Mapper.RESUME_WRITE_HANDLER.set(producer::unthrottle);

            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0L));

            final FlinkPravegaReader<Integer> pravegaSource = FlinkPravegaReader.<Integer>builder()
                    .forStream(streamName)
                    .withPravegaConfig(SETUP_UTILS.getPravegaConfig())
                    .withDeserializationSchema(new IntegerDeserializationSchema())
                    .build();

            env
                    .addSource(pravegaSource)
                    .map(new Mapper<>(SETUP_UTILS.getScope(), sideStream, SETUP_UTILS.getControllerUri().toString(), SETUP_UTILS.getClientConfig()))
                    .addSink(new IntSequenceExactlyOnceValidator(NUM_STREAM_ELEMENTS));

            try {
                env.execute();
            } catch (Exception e) {
                if (!(ExceptionUtils.getRootCause(e) instanceof SuccessException)) {
                    log.error("testReaderState failed with exception", e);
                    Assert.fail();
                }
            }
        }
    }

    @Slf4j
    public static class Mapper<T> implements MapFunction<T, T> {

        public static final AtomicReference<Runnable> RESUME_WRITE_HANDLER = new AtomicReference<>();
        private static final long serialVersionUID = 1L;
        private final String scope;
        private final String sideStream;
        private final String controllerUri;
        private final ClientConfig clientConfig;
        private transient EventStreamReader<Integer> sideStreamReader;

        public Mapper(String scope, String sideStream, String controllerUri, ClientConfig clientConfig) {
            this.scope = scope;
            this.sideStream = sideStream;
            this.controllerUri = controllerUri;
            this.clientConfig = clientConfig;
        }

        @Override
        public T map(T value) throws Exception {
            if (this.sideStreamReader == null) {
                this.sideStreamReader = getIntegerReader();
            }
            EventRead<Integer> rule = sideStreamReader.readNextEvent(50);
            if (rule.getEvent() != null) {
                log.info("Mapper: received side stream event: {}", rule.getEvent());
                /*
                 * Event == 1, continue process original events
                 * Event == 2, trigger an exception (simulate failure) and reset the writer thread and start processing all the records
                 */
                if (rule.getEvent() == 2) {
                    RESUME_WRITE_HANDLER.get().run();
                    throw new IntentionalException("artificial test failure");
                }
            }
            return value;
        }

        private EventStreamReader<Integer> getIntegerReader() {
            ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig);
            final String readerGroup = "side-reader-" + this.scope + "-" + sideStream;
            readerGroupManager.createReaderGroup(
                    readerGroup,
                    ReaderGroupConfig.builder().stream(Stream.of(this.scope, sideStream)).build());

            EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(this.scope, clientConfig);
            final String readerGroupId = UUID.randomUUID().toString();
            return clientFactory.createReader(
                    readerGroupId,
                    readerGroup,
                    new IntegerSerializer(),
                    ReaderConfig.builder().build());
        }
    }
}
