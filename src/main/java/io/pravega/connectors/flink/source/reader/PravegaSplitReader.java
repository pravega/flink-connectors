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

package io.pravega.connectors.flink.source.reader;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.TruncatedDataException;
import io.pravega.connectors.flink.source.PravegaSourceOptions;
import io.pravega.connectors.flink.source.split.PravegaSplit;
import io.pravega.connectors.flink.util.FlinkPravegaUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * A {@link SplitReader} implementation that reads records from Pravega. The split assigned to Pravega Split reader
 * represents a single Pravega EventStreamReader which will read events from Pravega stream accordingly.
 *
 * <p>The returned type are in the format of {@code EventRead(record)}.
 *
 */
@Internal
public class PravegaSplitReader
        implements SplitReader<EventRead<ByteBuffer>, PravegaSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(PravegaSplitReader.class);

    /**
     * Reader that reads event from Pravega stream.
     */
    private EventStreamReader<ByteBuffer> pravegaReader;

    /**
     * Split that PravegaSplitReader reads.
     */
    private PravegaSplit split;

    /**
     * Flink config options.
     */
    private final Configuration options;

    /**
     * Subtask ID of current Source Reader.
     */
    private final int subtaskId;

    /**
     * The supplied event stream client factory from Source Reader.
     */
    private final EventStreamClientFactory eventStreamClientFactory;

    /**
     * Creates a new Pravega Split Reader instance which can read event from Pravega stream.
     * The Pravega Split Reader is actually an instance of a {@link EventStreamReader}.
     *
     * @param scope                             The reader group scope name.
     * @param clientConfig                      The Pravega client configuration.
     * @param readerGroupName                   The reader group name.
     * @param subtaskId                         The subtaskId of source reader.
     */
    public PravegaSplitReader(
            String scope,
            ClientConfig clientConfig,
            String readerGroupName,
            int subtaskId) {
        this.subtaskId = subtaskId;
        this.options = new Configuration();
        this.eventStreamClientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        // create Pravega EventStreamReader
        try {
            this.pravegaReader = FlinkPravegaUtils.createPravegaReader(
                    PravegaSplit.splitId(subtaskId),
                    readerGroupName,
                    ReaderConfig.builder().build(),
                    eventStreamClientFactory);
        } catch (RuntimeException e) {
            LOG.error("Exception occurred while creating a Pravega EventStreamReader to read events", e);
            throw e;
        }
    }

    // read one or more event from an EventStreamReader
    @Override
    public RecordsWithSplitIds<EventRead<ByteBuffer>> fetch() {
        RecordsBySplits.Builder<EventRead<ByteBuffer>> records = new RecordsBySplits.Builder<>();
        EventRead<ByteBuffer> eventRead = null;

        // main work loop
        do {
            try {
                eventRead = pravegaReader.readNextEvent(
                        options.getLong(PravegaSourceOptions.READER_TIMEOUT_MS));
                LOG.debug("read event: {} on reader {}", eventRead.getEvent(), subtaskId);
            } catch (TruncatedDataException e) {
                // Data is truncated, Force the reader going forward to the next available event
                continue;
            } catch (IllegalStateException e) {
                // When catching an IllegalStateException means pravegaReader is closed,
                // indicating that wakeUp() was invoked upon a partial failure which we don't need
                // so that we return an empty RecordsBySplits to stop fetching and not break the recovering.
                return new RecordsBySplits.Builder<EventRead<ByteBuffer>>().build();
            }

            // push non-empty event to records queue
            if (eventRead.getEvent() != null || eventRead.isCheckpoint()) {
                records.add(split, eventRead);
            }
        } while (eventRead != null && !eventRead.isCheckpoint() && eventRead.getEvent() != null);
        return records.build();
    }

    // get the assigned split, we will only have one split for one reader as design for Pravega source
    @Override
    public void handleSplitsChanges(SplitsChange<PravegaSplit> splitsChange) {
        if (splitsChange instanceof SplitsAddition) {
            // ensure that one split reader is assigned only one split
            Preconditions.checkArgument(splitsChange.splits().size() == 1);
            this.split = splitsChange.splits().get(0);
        } else {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChange.getClass()));
        }
    }

    @Override
    public void wakeUp() {
        // this method is only called when
        // 1. Source Reader closes
        // 2. Split Enumerator assigns splits to Source Reader
        // for 1, the method close() will be called later
        // for 2, for Source Reader addSplits() will only be called once from the very beginning
        // while fetch task hasn't started yet, and it will not be called later
        // because one Source reader is one-to-one mapped with one split, thus we do nothing here
    }

    @Override
    public void close() throws Exception {
        Throwable ex = null;

        if (pravegaReader != null) {
            try {
                LOG.info("Closing Pravega reader");
                pravegaReader.close();
            } catch (Throwable e) {
                if (e instanceof InterruptedException) {
                    LOG.warn("Interrupted while waiting for Pravega reader to close, retrying ...");
                    pravegaReader.close();
                } else {
                    ex = ExceptionUtils.firstOrSuppressed(e, ex);
                }
            }
        }

        if (eventStreamClientFactory != null) {
            try {
                LOG.info("Closing Pravega eventStreamClientFactory");
                eventStreamClientFactory.close();
            } catch (Throwable e) {
                if (e instanceof InterruptedException) {
                    LOG.warn("Interrupted while waiting for eventStreamClientFactory to close, retrying ...");
                    eventStreamClientFactory.close();
                } else {
                    ex = ExceptionUtils.firstOrSuppressed(e, ex);
                }
            }
        }
        if (ex instanceof Exception) {
            throw (Exception) ex;
        }
    }
}
