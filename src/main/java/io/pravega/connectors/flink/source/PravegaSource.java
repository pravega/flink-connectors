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

package io.pravega.connectors.flink.source;

import io.pravega.client.ClientConfig;
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.connectors.flink.AbstractStreamingReaderBuilder;
import io.pravega.connectors.flink.serialization.CheckpointSerializer;
import io.pravega.connectors.flink.source.enumerator.PravegaSplitEnumerator;
import io.pravega.connectors.flink.source.reader.PravegaRecordEmitter;
import io.pravega.connectors.flink.source.reader.PravegaSourceReader;
import io.pravega.connectors.flink.source.reader.PravegaSplitReader;
import io.pravega.connectors.flink.source.split.PravegaSplit;
import io.pravega.connectors.flink.source.split.PravegaSplitSerializer;
import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * The Source implementation of Pravega. Please use a {@link PravegaSourceBuilder} to construct a {@link
 * PravegaSource}. The {@link PravegaSource} has two main components, {@link PravegaSplitEnumerator} and {@link PravegaSourceReader}.
 *
 * <p>The Split Enumerator will discover the splits(which represent Pravega EventStreamReaders) and then assign them to the Source Readers.
 * For Pravega, {@link PravegaSplitEnumerator} will assign splits of the same amount as the current parallelism to Source Readers,
 * while there will be one split assigned per Source Reader only.
 *
 * <p>The Source Reader will read the actual data. {@link PravegaSourceReader} encapsulates a Pravega EventStreamReader
 * from the split assigned by Split Enumerator, which will read events from Pravega stream.
 *
 * <p>The following example shows how to create a PravegaSource emitting records of <code>
 * Integer</code> type.
 *
 * <pre>{@code
 * PravegaSource<Integer> pravegaSource = PravegaSource.<Integer>builder()
 *                     .forStream(streamName)
 *                     .enableMetrics(false)
 *                     .withPravegaConfig(pravegaConfig)
 *                     .withReaderGroupName("flink-reader")
 *                     .withDeserializationSchema(new IntegerDeserializationSchema())
 *                     .build();
 * }</pre>
 *
 * @param <T> the output type of the source.
 */
@Experimental
@PublicEvolving
public class PravegaSource<T>
        implements Source<T, PravegaSplit, Checkpoint>, ResultTypeQueryable<T> {
    private static final Logger LOG = LoggerFactory.getLogger(PravegaSource.class);

    // The Pravega client config.
    final ClientConfig clientConfig;

    // The Pravega reader group config.
    final ReaderGroupConfig readerGroupConfig;

    // The scope name of the reader group.
    final String scope;

    // The readergroup name to colordinate the parallel readers. This should be unique for a Flink job.
    final String readerGroupName;

    // The supplied event deserializer.
    final DeserializationSchema<T> deserializationSchema;

    // the timeout for reading events from Pravega
    final Time eventReadTimeout;

    // the timeout for call that initiates the Pravega checkpoint
    final Time checkpointInitiateTimeout;

    // flag to enable/disable metrics
    final boolean enableMetrics;

    /**
     * Creates a new Pravega Source instance which can be added as a source to a Flink job.
     * It manages a reader group with a builder style constructor with user provided ReaderGroupConfig.
     * We can use {@link AbstractStreamingReaderBuilder} to build such a source.
     *
     * @param clientConfig              The Pravega client configuration.
     * @param readerGroupConfig         The Pravega reader group configuration.
     * @param scope                     The reader group scope name.
     * @param readerGroupName           The reader group name.
     * @param deserializationSchema     The implementation to deserialize events from Pravega streams.
     * @param eventReadTimeout          The event read timeout.
     * @param checkpointInitiateTimeout The checkpoint initiation timeout.
     * @param enableMetrics             Flag to indicate whether metrics needs to be enabled or not.
     */
    public PravegaSource(ClientConfig clientConfig,
                         ReaderGroupConfig readerGroupConfig, String scope, String readerGroupName,
                         DeserializationSchema<T> deserializationSchema,
                         Time eventReadTimeout, Time checkpointInitiateTimeout,
                         boolean enableMetrics) {
        this.clientConfig = Preconditions.checkNotNull(clientConfig, "clientConfig");
        this.readerGroupConfig = Preconditions.checkNotNull(readerGroupConfig, "readerGroupConfig");
        this.scope = Preconditions.checkNotNull(scope, "scope");
        this.readerGroupName = Preconditions.checkNotNull(readerGroupName, "readerGroupName");
        this.deserializationSchema = Preconditions.checkNotNull(deserializationSchema, "deserializationSchema");
        this.eventReadTimeout = Preconditions.checkNotNull(eventReadTimeout, "eventReadTimeout");
        this.checkpointInitiateTimeout = Preconditions.checkNotNull(checkpointInitiateTimeout, "checkpointInitiateTimeout");
        this.enableMetrics = enableMetrics;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Internal
    @Override
    public SourceReader<T, PravegaSplit> createReader(SourceReaderContext readerContext) {
        Supplier<PravegaSplitReader> splitReaderSupplier =
                () -> new PravegaSplitReader(scope, clientConfig,
                                readerGroupName, readerContext.getIndexOfSubtask());

        return new PravegaSourceReader<>(
                splitReaderSupplier,
                new PravegaRecordEmitter<>(deserializationSchema),
                new Configuration(),
                readerContext);
    }

    @Internal
    @Override
    public SplitEnumerator<PravegaSplit, Checkpoint> createEnumerator(
            SplitEnumeratorContext<PravegaSplit> enumContext) {
        return new PravegaSplitEnumerator(
                enumContext,
                this.scope,
                this.readerGroupName,
                this.clientConfig,
                this.readerGroupConfig,
                null);
    }

    @Internal
    @Override
    public SplitEnumerator<PravegaSplit, Checkpoint> restoreEnumerator(
            SplitEnumeratorContext<PravegaSplit> enumContext, Checkpoint checkpoint) throws IOException {
        return new PravegaSplitEnumerator(
                enumContext,
                this.scope,
                this.readerGroupName,
                this.clientConfig,
                this.readerGroupConfig,
                checkpoint);

    }

    @Internal
    @Override
    public SimpleVersionedSerializer<PravegaSplit> getSplitSerializer() {
        return new PravegaSplitSerializer();
    }

    @Internal
    @Override
    public SimpleVersionedSerializer<Checkpoint> getEnumeratorCheckpointSerializer() {
        return new CheckpointSerializer();
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return this.deserializationSchema.getProducedType();
    }

    // --------------- configurations -------------------------------
    /**
     * Gets a builder for {@link PravegaSource} to read Pravega streams using the Flink streaming API.
     * @param <T> the element type.
     * @return A new builder of {@link PravegaSource}
     */
    public static <T> PravegaSourceBuilder<T> builder() {
        return new PravegaSourceBuilder<>();
    }
}
