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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Orders elements into event time order using the watermark and managed state to buffer elements.
 *
 * @param <K>  Type of the keys
 * @param <T> The input type of the operator
 */
public class EventTimeOrderingOperator<K, T> extends AbstractStreamOperator<T>
        implements OneInputStreamOperator<T, T>, Triggerable<K, VoidNamespace>, InputTypeConfigurable {

    private static final long serialVersionUID = 1L;

    private static final String EVENT_QUEUE_STATE_NAME = "eventQueue";

    /**
     * The last seen watermark. This will be used to
     * decide if an incoming element is late or not.
     */
    @VisibleForTesting
    long lastWatermark = Long.MIN_VALUE;

    /**
     * The input type serializer for buffering events to managed state.
     */
    private TypeSerializer<T> inputSerializer;

    /**
     * The timer service.
     */
    private transient InternalTimerService<VoidNamespace> internalTimerService;

    /**
     * The queue of input elements keyed by event timestamp.
     */
    private transient MapState<Long, List<T>> elementQueueState;

    /**
     * Creates an event time-based reordering operator.
     */
    public EventTimeOrderingOperator() {
        chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
        this.inputSerializer = (TypeSerializer<T>) type.createSerializer(executionConfig);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        // create a map-based queue to buffer input elements
        if (elementQueueState == null) {
            elementQueueState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>(
                            EVENT_QUEUE_STATE_NAME,
                            LongSerializer.INSTANCE,
                            new ListSerializer<>(inputSerializer)
                    )
            );
        }
    }

    @Override
    public void open() throws Exception {
        super.open();
        internalTimerService = getInternalTimerService("ordering-timers", VoidNamespaceSerializer.INSTANCE, this);
    }

    @Override
    public void processElement(StreamRecord<T> element) throws Exception {
        if (!element.hasTimestamp()) {
            // elements with no time component are simply forwarded.
            // likely cause: the time characteristic of the program is not event-time.
            output.collect(element);
            return;
        }

        // In event-time processing we assume correctness of the watermark.
        // Events with timestamp smaller than (or equal to) the last seen watermark are considered late.
        // FUTURE: emit late elements to a side output

        if (element.getTimestamp() > lastWatermark) {
            // we have an event with a valid timestamp, so
            // we buffer it until we receive the proper watermark.
            saveRegisterWatermarkTimer();
            bufferEvent(element);
        }
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        super.processWatermark(mark);
        lastWatermark = mark.getTimestamp();
    }

    /**
     * Buffers an element for future processing.
     *
     * @param element the element to buffer.
     * @throws Exception if any error occurs.
     */
    private void bufferEvent(StreamRecord<T> element) throws Exception {

        assert element.hasTimestamp();
        long timestamp = element.getTimestamp();

        List<T> elementsForTimestamp = elementQueueState.get(timestamp);
        if (elementsForTimestamp == null) {
            elementsForTimestamp = new ArrayList<>(1);
        }

        if (getRuntimeContext().getExecutionConfig().isObjectReuseEnabled()) {
            // copy the object so that the original object may be reused
            elementsForTimestamp.add(inputSerializer.copy(element.getValue()));
        } else {
            elementsForTimestamp.add(element.getValue());
        }
        elementQueueState.put(timestamp, elementsForTimestamp);
    }

    /**
     * Registers a timer for {@code current watermark + 1}, this means that we get triggered
     * whenever the watermark advances, which is what we want for working off the queue of
     * buffered elements.
     */
    private void saveRegisterWatermarkTimer() {
        long currentWatermark = internalTimerService.currentWatermark();
        // protect against overflow
        if (currentWatermark + 1 > currentWatermark) {
            internalTimerService.registerEventTimeTimer(VoidNamespace.INSTANCE, currentWatermark + 1);
        }
    }

    /**
     * Occurs when an event-time timer fires due to watermark progression.
     *
     * @param timer the timer details.
     */
    @Override
    public void onEventTime(InternalTimer<K, VoidNamespace> timer) throws Exception {

        long currentWatermark = internalTimerService.currentWatermark();

        PriorityQueue<Long> sortedTimestamps = getSortedTimestamps();
        while (!sortedTimestamps.isEmpty() && sortedTimestamps.peek() <= currentWatermark) {
            long timestamp = sortedTimestamps.poll();
            for (T event : elementQueueState.get(timestamp)) {
                output.collect(new StreamRecord<>(event, timestamp));
            }
            elementQueueState.remove(timestamp);
        }

        if (sortedTimestamps.isEmpty()) {
            elementQueueState.clear();
        }

        if (!sortedTimestamps.isEmpty()) {
            saveRegisterWatermarkTimer();
        }
    }

    @Override
    public void onProcessingTime(InternalTimer<K, VoidNamespace> timer) throws Exception {
    }

    /**
     * Gets the sorted timestamps of any buffered events.
     *
     * @return a sorted list of timestamps that have at least one buffered event.
     */
    private PriorityQueue<Long> getSortedTimestamps() throws Exception {
        PriorityQueue<Long> sortedTimestamps = new PriorityQueue<>();
        for (Long timestamp : elementQueueState.keys()) {
            sortedTimestamps.offer(timestamp);
        }
        return sortedTimestamps;
    }
}
