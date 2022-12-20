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
package io.pravega.connectors.flink.sink;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.PravegaEventRouter;
import io.pravega.connectors.flink.PravegaWriterMode;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A Pravega {@link org.apache.flink.api.connector.sink2.SinkWriter} implementation that is suitable for
 * {@link PravegaWriterMode#BEST_EFFORT} and {@link PravegaWriterMode#ATLEAST_ONCE}.
 *
 * <p>Note that the difference between these two modes is that {@link PravegaEventWriter#flushAndVerify()}
 * is called for each checkpoint in the {@link PravegaWriterMode#ATLEAST_ONCE} mode.
 *
 * @param <T> The type of the event to be written.
 */
public class PravegaEventWriter<T> implements SinkWriter<T> {
    private static final Logger LOG = LoggerFactory.getLogger(PravegaEventWriter.class);

    // Error which will be detected asynchronously and reported to Flink
    @VisibleForTesting
    protected volatile AtomicReference<Throwable> writeError = new AtomicReference<>(null);

    // Used to track confirmation from all writes to ensure guaranteed writes.
    @VisibleForTesting
    protected AtomicLong pendingWritesCount = new AtomicLong();

    // The async executor
    @VisibleForTesting
    protected transient ExecutorService executorService;

    // Client factory for PravegaEventWriter instances
    @VisibleForTesting
    protected transient EventStreamClientFactory clientFactory;

    // The Pravega client config.
    private final ClientConfig clientConfig;

    // The destination stream.
    @SuppressFBWarnings("SE_BAD_FIELD")
    private final Stream stream;

    // The sink's mode of operation. This is used to provide different guarantees for the written events.
    private final PravegaWriterMode writerMode;

    // The supplied event serializer.
    private final SerializationSchema<T> serializationSchema;

    // The router used to partition events within a stream, can be null for random routing
    @Nullable
    private final PravegaEventRouter<T> eventRouter;

    // Pravega writer instance
    private final transient EventStreamWriter<T> writer;

    // The writer id
    private final String writerId;

    // flag to enable/disable metrics
    private final boolean enableMetrics;

    // The total number of output records
    private final Counter numRecordsOutCounter;

    // The total number of records failed to send
    private final Counter numRecordsOutErrorsCounter;

    /**
     * A Pravega non-transactional writer that handles {@link PravegaWriterMode#BEST_EFFORT} and
     * {@link PravegaWriterMode#ATLEAST_ONCE} writer mode.
     *
     * @param context               Some runtime info from sink.
     * @param clientConfig          The Pravega client configuration.
     * @param stream                The destination stream.
     * @param writerMode            The Pravega writer mode.
     * @param serializationSchema   The implementation for serializing every event into pravega's storage format.
     * @param eventRouter           The implementation to extract the partition key from the event.
     * @param enableMetrics         Flag to indicate whether metrics needs to be enabled or not.
     */
    public PravegaEventWriter(Sink.InitContext context,
                              ClientConfig clientConfig,
                              Stream stream,
                              PravegaWriterMode writerMode,
                              SerializationSchema<T> serializationSchema,
                              PravegaEventRouter<T> eventRouter,
                              boolean enableMetrics) {
        this.clientConfig = clientConfig;
        this.stream = stream;
        this.writerMode = writerMode;
        this.serializationSchema = serializationSchema;
        this.eventRouter = eventRouter;
        this.writer = initializeInternalWriter();
        this.writerId = UUID.randomUUID() + "-" + context.getSubtaskId();
        this.enableMetrics = enableMetrics;
        this.numRecordsOutCounter = context.metricGroup().getIOMetricGroup().getNumRecordsOutCounter();
        this.numRecordsOutErrorsCounter = context.metricGroup().getNumRecordsOutErrorsCounter();

        LOG.info("Initialized Pravega writer {} for stream: {} with controller URI: {}",
                writerId, stream, clientConfig.getControllerURI());
    }

    @VisibleForTesting
    protected EventStreamWriter<T> initializeInternalWriter() {
        clientFactory = EventStreamClientFactory.withScope(stream.getScope(), clientConfig);
        Serializer<T> eventSerializer = new FlinkSerializer<>(serializationSchema);
        EventWriterConfig writerConfig = EventWriterConfig.builder().build();
        executorService = Executors.newSingleThreadExecutor();
        return clientFactory.createEventWriter(stream.getStreamName(), eventSerializer, writerConfig);
    }

    @Override
    public void write(T element, Context context) throws IOException, InterruptedException {
        checkWriteError();

        pendingWritesCount.incrementAndGet();
        final CompletableFuture<Void> future;
        if (eventRouter != null) {
            future = writer.writeEvent(eventRouter.getRoutingKey(element), element);
        } else {
            future = writer.writeEvent(element);
        }

        future.whenCompleteAsync(
                (result, e) -> {
                    if (e != null) {
                        LOG.warn("Detected a write failure", e);
                        if (enableMetrics) {
                            numRecordsOutErrorsCounter.inc();
                        }

                        // We will record only the first error detected, since this will mostly likely help with
                        // finding the root cause. Storing all errors will not be feasible.
                        writeError.compareAndSet(null, e);
                    }
                    synchronized (this) {
                        pendingWritesCount.decrementAndGet();
                        this.notify();
                    }
                },
                executorService
        );

        if (enableMetrics) {
            numRecordsOutCounter.inc();
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (writerMode == PravegaWriterMode.ATLEAST_ONCE) {
            flushAndVerify();
        }
    }

    @VisibleForTesting
    public void flushAndVerify() throws IOException, InterruptedException {
        writer.flush();

        // Wait until all errors, if any, have been recorded.
        synchronized (this) {
            while (this.pendingWritesCount.get() > 0) {
                this.wait();
            }
        }

        checkWriteError();
    }

    private void checkWriteError() throws IOException {
        Throwable error = writeError.getAndSet(null);
        if (error != null) {
            throw new IOException("Write failure", error);
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("{} - Close the PravegaEventWriter", writerId);

        Exception exception = null;

        if (writer != null) {
            try {
                flushAndVerify();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }

            try {
                writer.close();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }

            try {
                executorService.shutdown();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }
        }

        if (clientFactory != null) {
            try {
                clientFactory.close();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }
        }

        if (exception != null) {
            throw exception;
        }
    }

    @VisibleForTesting
    protected PravegaWriterMode getWriterMode() {
        return writerMode;
    }

    @VisibleForTesting
    @Nullable
    protected PravegaEventRouter<T> getEventRouter() {
        return eventRouter;
    }

    @VisibleForTesting
    protected EventStreamWriter<T> getInternalWriter() {
        return writer;
    }
}
