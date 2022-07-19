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

package io.pravega.connectors.flink.utils.runtime;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.utils.IntegerSerializer;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.UUID;

/**
 * A Pravega cluster operator is used for operating Pravega instance.
 */
public class PravegaRuntimeOperator implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(PravegaRuntimeOperator.class);

    private final URI controllerUri;
    private final String scope;
    private final String containerId;
    private final EventStreamClientFactory eventStreamClientFactory;

    public PravegaRuntimeOperator(String scope, String controllerUri, String containerId) {
        this.scope = scope;
        this.controllerUri = URI.create(controllerUri);
        this.containerId = containerId;
        this.eventStreamClientFactory = EventStreamClientFactory.withScope(this.scope, getClientConfig());
    }

    /**
     * Create the test stream with the given segment number.
     *
     * @param streamName     Name of the test stream.
     * @param numSegments    Number of segments to be created for this stream.
     *
     * @throws Exception on any errors.
     */
    public void createTestStream(final String streamName, final int numSegments)
            throws Exception {
        Preconditions.checkNotNull(streamName);
        Preconditions.checkArgument(numSegments > 0);

        try (StreamManager streamManager = StreamManager.create(getClientConfig())) {
            streamManager.createScope(this.scope);
            streamManager.createStream(this.scope, streamName,
                    StreamConfiguration.builder()
                            .scalingPolicy(ScalingPolicy.fixed(numSegments))
                            .build());
            LOG.info("Created stream: " + streamName);
        }
    }

    /**
     * Return the stream instance.
     *
     * @param streamName     Name of the test stream.
     *
     * @return a Stream
     */
    public Stream getStream(final String streamName) {
        return Stream.of(this.scope, streamName);
    }

    /**
     * Create a stream writer for writing Integer events.
     *
     * @param streamName    Name of the test stream.
     *
     * @return Stream writer instance.
     */
    public EventStreamWriter<Integer> getIntegerWriter(final String streamName) {
        Preconditions.checkNotNull(streamName);

        return eventStreamClientFactory.createEventWriter(
                streamName,
                new IntegerSerializer(),
                EventWriterConfig.builder().build());
    }

    /**
     * Create a stream reader for reading Integer events.
     *
     * @param streamName    Name of the test stream.
     *
     * @return Stream reader instance.
     */
    public EventStreamReader<Integer> getIntegerReader(final String streamName) {
        Preconditions.checkNotNull(streamName);

        final String readerGroup = "testReaderGroup" + this.scope + streamName;

        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(this.scope, getClientConfig())) {
            readerGroupManager.createReaderGroup(
                    readerGroup,
                    ReaderGroupConfig.builder().stream(Stream.of(this.scope, streamName)).build());
        }

        final String readerGroupId = UUID.randomUUID().toString();
        return eventStreamClientFactory.createReader(
                readerGroupId,
                readerGroup,
                new IntegerSerializer(),
                ReaderConfig.builder().build());
    }

    /** Return the controller URI for this Pravega runtime. */
    public URI getControllerUri() {
        return controllerUri;
    }

    /** Return the generated scope of this Pravega runtime. It is used in tests. */
    public String getScope() {
        return scope;
    }

    /** The configuration for this Pravega runtime. */
    public PravegaConfig getPravegaConfig() {
        return PravegaConfig.fromDefaults()
                .withControllerURI(this.controllerUri)
                .withDefaultScope(this.scope);
    }

    /** The client configuration for this Pravega runtime. */
    public ClientConfig getClientConfig() {
        return getPravegaConfig().getClientConfig();
    }

    public String getContainerId() {
        return containerId;
    }

    @Override
    public void close() throws IOException {
        if (eventStreamClientFactory != null) {
            eventStreamClientFactory.close();
        }
    }
}
