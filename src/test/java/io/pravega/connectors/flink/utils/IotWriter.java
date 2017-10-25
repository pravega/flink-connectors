/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.flink.utils;

import io.pravega.client.ClientFactory;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.connectors.flink.util.StreamId;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;

import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A IOT-inspired writer for testing purposes.
 *
 * <p>Generates a stream of (slightly out-of-order) sensor events, together with segment splits/merges.
 */
@Slf4j
public class IotWriter extends AbstractStreamBasedWriter<IotWriter.SensorEvent> {

    public IotWriter(ClientFactory clientFactory, Controller controllerClient, StreamId streamId) {
        super(clientFactory, controllerClient, streamId, e -> e.f1, new JavaSerializer<>());
    }

    @Override
    protected List<java.util.stream.Stream<SensorEvent>> createStreams() {
        return Arrays.stream(Sensor.values()).map(this::getSensorEvents).collect(Collectors.toList());
    }

    protected java.util.stream.Stream<SensorEvent> getSensorEvents(final Sensor sensor) {
        return Stream
                .generate(() -> new SensorEvent(sensor.getClock().instant().toEpochMilli(), sensor.getId()))
                .limit(100);
    }

    public enum Sensor {
        // define a few sensors with associated event-time clocks (with varying degrees of clock skew)
        A("A", Duration.ofSeconds(-5)),
        B("B", Duration.ofSeconds(+5)),
        C("C", Duration.ofSeconds(-10));

        @Getter
        private final String id;

        @Getter
        private final Duration clockSkew;

        Sensor(String id, Duration clockSkew) {
            this.id = id;
            this.clockSkew = clockSkew;
            this.clock = Clock.offset(ingestionClock, clockSkew);
        }

        @Getter
        private final Clock clock;
    }

    public static class SensorEvent extends Tuple2<Long, String> {
        public SensorEvent(Long timestamp, String sensorId) {
            super(timestamp, sensorId);
        }
    }
}
