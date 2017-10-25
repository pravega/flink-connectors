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

import io.pravega.client.ClientFactory;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.connectors.flink.serialization.PravegaDeserializationSchema;
import io.pravega.connectors.flink.util.StreamId;
import io.pravega.connectors.flink.utils.IotWriter;
import io.pravega.connectors.flink.utils.SetupUtils;
import io.pravega.connectors.flink.utils.SuccessException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;

/**
 * Integration tests for {@link FlinkPravegaReader} event time support.
 */
@Slf4j
public class EventTimeITCase extends StreamingMultipleProgramsTestBase {

    // Setup utility
    private static final SetupUtils SETUP_UTILS = new SetupUtils();

    //Ensure each test completes within 120 seconds.
//    @Rule
//    public final Timeout globalTimeout = new Timeout(120, TimeUnit.SECONDS);
//
    @BeforeClass
    public static void setupPravega() throws Exception {
        SETUP_UTILS.startAllServices();
    }

    @AfterClass
    public static void tearDownPravega() throws Exception {
        SETUP_UTILS.stopAllServices();
    }

    private Controller controllerClient;
    private ClientFactory clientFactory;
    private IotWriter producer;

    @Before
    public void before() throws Exception {
        controllerClient = SETUP_UTILS.newController();
        clientFactory = SETUP_UTILS.newClientFactory();
    }

    @After
    public void after() throws Exception {
        if (clientFactory != null) {
            clientFactory.close();
        }
        if (controllerClient != null) {
            controllerClient.close();
        }
    }

    @Test
    public void testSampleWriter() throws Exception {
        // set up the stream
        final StreamId streamId = new StreamId(SETUP_UTILS.getScope(), RandomStringUtils.randomAlphabetic(20));
        SETUP_UTILS.createTestStream(streamId.getName(), 2);
        IotWriter writer = new IotWriter(clientFactory, controllerClient, streamId);
        writer.start().join();
        writer.close();
    }

    @Test
    @Ignore
    public void testEventTimeWatermarking() throws Exception {

        // set up the stream
        final StreamId streamId = new StreamId(SETUP_UTILS.getScope(), RandomStringUtils.randomAlphabetic(20));
        SETUP_UTILS.createTestStream(streamId.getName(), 2);

        try (IotWriter writer = new IotWriter(clientFactory, controllerClient, streamId)) {


            // configure the stream environment
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            env.setRestartStrategy(RestartStrategies.noRestart());
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

            // create the Pravega reader
            final FlinkPravegaReader<IotWriter.SensorEvent> pravegaSource = new FlinkPravegaReader<>(
                    SETUP_UTILS.getControllerUri(),
                    SETUP_UTILS.getScope(),
                    Collections.singleton(streamId.getName()),
                    0,
                    new PravegaDeserializationSchema<>(IotWriter.SensorEvent.class, new JavaSerializer<>()));

            pravegaSource.setEventTimeSkew(Duration.ofSeconds(1));

            // create an operator that reorders events into event time (by key)
            EventTimeOrderingOperator<String, IotWriter.SensorEvent> orderer = new EventTimeOrderingOperator<>();

            // assemble the dataflow
            env
                    .addSource(pravegaSource)
                    .transform("reorder", TypeInformation.of(IotWriter.SensorEvent.class), orderer).forward()
                    .print();


            // start producing records
            writer.start().handle((v, e) -> {
                if (e != null) {
                    e.printStackTrace();
                }
                return null;
            });

            final long executeStart = System.nanoTime();

            // if these calls complete without exception, then the test passes
            try {
                env.execute();
            } catch (Exception e) {
                if (!(ExceptionUtils.getRootCause(e) instanceof SuccessException)) {
                    throw e;
                }
            }

            final long executeEnd = System.nanoTime();
            System.out.println(String.format("Test execution took %d ms", (executeEnd - executeStart) / 1_000_000));
        }
    }

    // ----------------------------------------------------------------------------

}