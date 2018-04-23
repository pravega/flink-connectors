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

import com.google.common.util.concurrent.AtomicLongMap;
import io.pravega.client.ClientFactory;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.connectors.flink.serialization.PravegaDeserializationSchema;
import io.pravega.connectors.flink.util.StreamId;
import io.pravega.connectors.flink.utils.IotWriter;
import io.pravega.connectors.flink.utils.SetupUtils;
import io.pravega.connectors.flink.utils.SuccessException;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * Integration tests for {@link FlinkPravegaReader} event time support.
 */
@Slf4j
public class EventTimeITCase extends StreamingMultipleProgramsTestBase {

    // Setup utility
    private static final SetupUtils SETUP_UTILS = new SetupUtils();

    //Ensure each test completes within 120 seconds.
    @Rule
    public final Timeout globalTimeout = new Timeout(5, TimeUnit.MINUTES);

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
    public void testEventTimeWatermarking() throws Exception {

        // set up the stream
        final StreamId streamId = new StreamId(SETUP_UTILS.getScope(), RandomStringUtils.randomAlphabetic(20));
        SETUP_UTILS.createTestStream(streamId.getName(), 2);

        @Cleanup
        IotWriter writer = new IotWriter(clientFactory, controllerClient, streamId);
        writer.setLimit(30);
        writer.setScalePeriod(Duration.ofSeconds(60));
        writer.setWriteThrottle(Duration.ofSeconds(1));
        writer.start();

        // configure the stream environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(Duration.ofSeconds(15).toMillis());

        // create the Pravega reader
        final FlinkPravegaReader<IotWriter.SensorEvent> pravegaSource = new FlinkPravegaReader<>(
                SETUP_UTILS.getControllerUri(),
                SETUP_UTILS.getScope(),
                Collections.singleton(streamId.getName()),
                0,
                new PravegaDeserializationSchema<>(IotWriter.SensorEvent.class, new JavaSerializer<>()));

        // set the maximum lateness of records in event time
        pravegaSource.setEventTimeLag(Duration.ofSeconds(10));
        pravegaSource.setTimestampAssigner((evt, t) -> evt.f0);

        // create an operator that reorders events into event time (by key)
        EventTimeOrderingOperator<String, IotWriter.SensorEvent> orderer = new EventTimeOrderingOperator<>();

        // assemble the dataflow
        env
                .addSource(pravegaSource)
                .keyBy(evt -> evt.f1)
                .transform("reorder", TypeInformation.of(IotWriter.SensorEvent.class), orderer).forward()
                .map(new SensorEventValidator(writer.getLimit(), pravegaSource.getEventTimeLag()))
                .print();

        // if these calls complete without exception, then the test passes
        try {
            env.execute();
        } catch (Exception e) {
            if (!(ExceptionUtils.getRootCause(e) instanceof SuccessException)) {
                throw e;
            }
        }
    }

    // ----------------------------------------------------------------------------

    /**
     * Validates that the expected number of sensor events arrive at the validator (or none if the sensor produces late records).
     */
    private static class SensorEventValidator extends RichMapFunction<IotWriter.SensorEvent, IotWriter.SensorEvent> {

        private final long limit;
        private final Duration eventTimeSkew;

        private transient AtomicLongMap<String> remaining;

        public SensorEventValidator(long limit, Duration eventTimeSkew) {
            this.limit = limit;
            this.eventTimeSkew = eventTimeSkew;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            remaining = AtomicLongMap.create();
            // use the event time skew to identity which sensors to expect late records from
            for(IotWriter.Sensor sensor : IotWriter.Sensor.values()) {
                boolean late = sensor.getClockSkew().negated().compareTo(eventTimeSkew) > 0;
                remaining.put(sensor.getId(), late ? 0 : limit);
            }
        }

        @Override
        public IotWriter.SensorEvent map(IotWriter.SensorEvent value) throws Exception {
            if (remaining.getAndDecrement(value.f1) <= 0) {
                Assert.fail("Received an unexpected event: " + value);
            }
            remaining.removeAllZeros();
            if (remaining.isEmpty()) {
                throw new SuccessException();
            }
            return value;
        }
    }
}