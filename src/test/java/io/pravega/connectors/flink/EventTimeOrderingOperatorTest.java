/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.connectors.flink;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link EventTimeOrderingOperator}.
 */
@Slf4j
public class EventTimeOrderingOperatorTest {

    private static final String K1 = "K1";

    private EventTimeOrderingOperator<String, Tuple2<String, Long>> operator;
    private KeyedOneInputStreamOperatorTestHarness<String, Tuple2<String, Long>, Tuple2<String, Long>> testHarness;

    @Before
    public void before() throws Exception {
        operator = new EventTimeOrderingOperator<>();
        operator.setInputType(TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
        }), new ExecutionConfig());
        testHarness = new KeyedOneInputStreamOperatorTestHarness<>(
                operator, in -> in.f0, TypeInformation.of(String.class));
        testHarness.setTimeCharacteristic(TimeCharacteristic.EventTime);
        testHarness.open();
    }

    @After
    public void after() throws Exception {
        testHarness.close();
        operator.close();
    }

    @Test
    public void testOrdering() throws Exception {

        Queue<Object> actual;
        Queue<Object> expected;

        // emit some out of order events for a given key.
        // numerous events are emitted for timestamp 2 to validate support for having
        // more than one event at a given timestamp.
        testHarness.processElement(record(K1, 1L));
        testHarness.processElement(record(K1, 3L));
        testHarness.processElement(record(K1, 2L));
        testHarness.processElement(record(K1, 2L));
        testHarness.processElement(record(K1, 4L));

        // advance to timestamp 3, expecting a subset of elements to be emitted.
        testHarness.processWatermark(3L);
        actual = testHarness.getOutput();
        expected = new ConcurrentLinkedQueue<>();
        expected.add(record(K1, 1L));
        expected.add(record(K1, 2L));
        expected.add(record(K1, 2L));
        expected.add(record(K1, 3L));
        expected.add(watermark(3L));
        TestHarnessUtil.assertOutputEquals("Unexpected output", expected, actual);
        actual.clear();

        // advance to timestamp 4, expecting the final element to be emitted.
        testHarness.processWatermark(4L);
        actual = testHarness.getOutput();
        expected = new ConcurrentLinkedQueue<>();
        expected.add(record(K1, 4L));
        expected.add(watermark(4L));
        TestHarnessUtil.assertOutputEquals("Unexpected output", expected, actual);
        actual.clear();

        // advance to timestamp 5, expecting no elements to be emitted.
        testHarness.processWatermark(5L);
        actual = testHarness.getOutput();
        expected = new ConcurrentLinkedQueue<>();
        expected.add(watermark(5L));
        TestHarnessUtil.assertOutputEquals("Unexpected output", expected, actual);
        actual.clear();
    }

    @Test
    public void testLateElements() throws Exception {
        testHarness.processWatermark(1L);
        assertEquals(1L, operator.lastWatermark);
        testHarness.processElement(record(K1, 0L));
        testHarness.processElement(record(K1, 1L));
        testHarness.processWatermark(2L);
        assertEquals(2L, operator.lastWatermark);

        Queue<Object> actual = testHarness.getOutput();
        Queue<Object> expected = new ConcurrentLinkedQueue<>();
        expected.add(watermark(1L));
        expected.add(watermark(2L));
        TestHarnessUtil.assertOutputEquals("Unexpected output", expected, actual);
    }

    @Test
    public void testProcessingTime() throws Exception {
        testHarness.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        testHarness.processElement(new StreamRecord<>(new Tuple2<>(K1, 0L)));
        Queue<Object> actual = testHarness.getOutput();
        Queue<Object> expected = new ConcurrentLinkedQueue<>();
        expected.add(new StreamRecord<>(new Tuple2<>(K1, 0L)));
        TestHarnessUtil.assertOutputEquals("Unexpected output", expected, actual);
    }

    // ------ utility methods

    private static StreamRecord<Tuple2<String, Long>> record(String key, long timestamp) {
        return new StreamRecord<>(new Tuple2<>(key, timestamp), timestamp);
    }

    private static Watermark watermark(long timestamp) {
        return new Watermark(timestamp);
    }
}