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

import com.google.common.base.Preconditions;
import io.pravega.client.ClientFactory;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.stream.impl.StreamSegments;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.connectors.flink.PravegaEventRouter;
import io.pravega.connectors.flink.util.StreamId;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Writes one or more Java 8 {@link Stream streams} to a Pravega stream.
 */
@Slf4j
public abstract class AbstractStreamBasedWriter<T> implements AutoCloseable {

    // the ingestion-time clock (the system clock)
    protected static final Clock ingestionClock = Clock.systemDefaultZone();

    // parameters
    protected final ClientFactory clientFactory;
    protected final Controller controllerClient;
    protected final StreamId streamId;
    private final PravegaEventRouter<T> eventRouter;
    private final Serializer<T> eventSerializer;

    private Duration scalePeriod = Duration.ofSeconds(10);
    private Duration writeThrottle = Duration.ofSeconds(1);

    private final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(1, this.getClass().getName());

    // the underlying Pravega writer
    private final EventStreamWriter<T> writer;

    private volatile CompletableFuture<Void> result;

    // indicates when last the stream was scaled
    private volatile Instant lastScaleTime = Instant.EPOCH;

    protected AbstractStreamBasedWriter(ClientFactory clientFactory, Controller controllerClient, StreamId streamId, PravegaEventRouter<T> eventRouter, Serializer<T> eventSerializer) {
        this.clientFactory = Preconditions.checkNotNull(clientFactory);
        this.controllerClient = Preconditions.checkNotNull(controllerClient);
        this.streamId = Preconditions.checkNotNull(streamId);
        this.eventRouter = eventRouter;
        this.eventSerializer = eventSerializer;

        writer = newWriter();
    }

    // region Properties

    public void setWriteThrottle(Duration writeThrottle) {
        this.writeThrottle = writeThrottle;
    }

    public void setScalePeriod(Duration scalePeriod) {
        this.scalePeriod = scalePeriod;
    }

    // endregion

    // region Lifecycle

    @Synchronized
    public CompletableFuture<Void> start() {
        Preconditions.checkState(!executor.isShutdown(), "Already closed");
        Preconditions.checkState(result == null, "Already started");

        result = new CompletableFuture<>();
        FutureHelpers.completeAfter(this::writeStreams, result);
        return result;
    }

    @Synchronized
    public boolean cancel() {
        if (result != null && !result.isDone()) {
            return result.cancel(true);
        }
        else {
            return true;
        }
    }

    @Synchronized
    public boolean isRunning() {
        return result != null && !result.isDone();
    }

    @Synchronized
    @Override
    public void close() throws Exception {
        if(result != null) {
            result.cancel(true);
//            result.join();
        }
        executor.shutdown();
    }

    // endregion

    // region Writing

    private EventStreamWriter<T> newWriter() {
        return clientFactory.createEventWriter(
                streamId.getName(),
                eventSerializer,
                EventWriterConfig.builder().retryAttempts(1).build());
    }

    @Synchronized
    private CompletableFuture<Void> writeStreams() {
        final List<java.util.stream.Stream<T>> streams = createStreams();
        lastScaleTime = ingestionClock.instant();
        Iterator<T> iter = new MergeIterator<>(streams.stream().map(Stream::iterator).collect(Collectors.toList()));
        return writeAll(iter).whenComplete((v, th) -> {
            streams.forEach(Stream::close);
            log.debug("writeStreams completed");
        });
    }

    @Synchronized
    private CompletableFuture<Void> writeAll(final Iterator<T> iterator) {
        if (result.isCancelled() || !iterator.hasNext()) {
            return CompletableFuture.completedFuture(null);
        }
        return scaleIfRequired()
                .thenCompose(v -> writeNext(iterator))
                .thenComposeAsync(v -> {
                    try {
                        Thread.sleep(writeThrottle.toMillis());
                    } catch (InterruptedException e) {
                        return CompletableFuture.completedFuture(null);
                    }
                    return writeAll(iterator);
                }, executor);
    }

    @Synchronized
    private CompletableFuture<Void> writeNext(final Iterator<T> iterator) {
        assert iterator.hasNext();
        final T event = iterator.next();
        log.debug("Writing: {}", event);
        onWriteEvent(event);
        return writer
                .writeEvent(eventRouter.getRoutingKey(event), event)
                .whenComplete((v,th) -> {
                    if (th != null) {
                        throw new CompletionException("writeNext failed", th);
                    }
                    log.trace("Wrote: {}", event);
                });
    }

    protected void onWriteEvent(T event) {

    }

    // endregion

    // region Scaling

    private CompletableFuture<Void> scaleIfRequired() {
        final Instant now = ingestionClock.instant();
        if (scalePeriod.isZero() || now.isBefore(lastScaleTime.plus(scalePeriod))) {
            return CompletableFuture.completedFuture(null);
        }
        log.debug("Scaling the stream...");
        final io.pravega.client.stream.Stream stream = new StreamImpl(streamId.getScope(), streamId.getName());
        return controllerClient.getCurrentSegments(streamId.getScope(), streamId.getName())
                .thenCompose(segments -> {
                    if(segments.getSegments().size() == 1) {
                        // split
                        Map<Double, Double> keyRanges = new HashMap<>();
                        keyRanges.put(0.0, 0.5);
                        keyRanges.put(0.5, 1.0);
                        return controllerClient.scaleStream(stream, getSegmentIds(segments), keyRanges, executor).getFuture();
                    }
                    else {
                        // merge
                        return controllerClient.scaleStream(stream, getSegmentIds(segments), Collections.singletonMap(0.0, 1.0), executor).getFuture();
                    }
                })
                .handle((success, th) -> {
                    if (th != null) {
                        throw new CompletionException("scaleIfRequired failed", th);
                    }
                    if (success == null || !success) {
                       throw new RuntimeException("scaleIfRequired failed: scale operation failed");
                    }
                    log.debug("Scale complete.");
                    lastScaleTime = now;
                    return null;
                });
    }

    private static List<Integer> getSegmentIds(StreamSegments segments) {
        return segments.getSegments().stream().map(Segment::getSegmentNumber).collect(Collectors.toList());
    }

    // endregion

    /**
     * Creates the Java 8 stream(s) to write to the Pravega stream.
     * @return
     */
    protected abstract List<java.util.stream.Stream<T>> createStreams();
}
