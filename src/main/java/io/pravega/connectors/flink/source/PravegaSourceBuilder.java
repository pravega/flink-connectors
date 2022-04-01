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

import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.PravegaOptions;
import io.pravega.connectors.flink.util.FlinkPravegaUtils;
import io.pravega.connectors.flink.watermark.AssignerWithTimeWindows;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static io.pravega.connectors.flink.PravegaOptions.buildClientConfigFromProperties;
import static io.pravega.connectors.flink.PravegaOptions.getPropertiesFromEnvironmentAndCommand;

/**
 *The @builder class for {@link PravegaSource} to make it easier for the users to construct a {@link
 *  PravegaSource}.
 *
 * @param <T> the element type.
 */
public class PravegaSourceBuilder<T> {

    private DeserializationSchema<T> deserializationSchema;
    private @Nullable SerializedValue<AssignerWithTimeWindows<T>> assignerWithTimeWindows;
    /**
     * The internal Pravega client configuration. See {@link PravegaOptions}.
     */
    private final Properties pravegaClientConfig = new Properties();
    private final List<Triple<String, StreamCut, StreamCut>> streams = new ArrayList<>(1);
    private boolean enableMetrics = true;
    private Time checkpointInitiateTimeout = Time.seconds(5);
    private Time eventReadTimeout = Time.seconds(1);
    private @Nullable String readerGroupScope;
    private @Nullable String readerGroupName;
    private @Nullable Time readerGroupRefreshTime;
    private int maxOutstandingCheckpointRequest = 3;

    protected PravegaSourceBuilder<T> builder() {
        return this;
    }

    /**
     * Sets the deserialization schema.
     *
     * @param deserializationSchema The deserialization schema
     * @return Builder instance.
     */
    public PravegaSourceBuilder<T> withDeserializationSchema(DeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
        return builder();
    }

    /**
     * Sets the timestamp and watermark assigner.
     *
     * @param assignerWithTimeWindows The timestamp and watermark assigner.
     * @return Builder instance.
     */
    public PravegaSourceBuilder<T> withTimestampAssigner(AssignerWithTimeWindows<T> assignerWithTimeWindows) {
        try {
            ClosureCleaner.clean(assignerWithTimeWindows, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
            this.assignerWithTimeWindows = new SerializedValue<>(assignerWithTimeWindows);
        } catch (IOException e) {
            throw new IllegalArgumentException("The given assigner is not serializable", e);
        }
        return this;
    }

    protected DeserializationSchema<T> getDeserializationSchema() {
        Preconditions.checkState(deserializationSchema != null, "Deserialization schema must not be null.");
        return deserializationSchema;
    }

    protected SerializedValue<AssignerWithTimeWindows<T>> getAssignerWithTimeWindows() {
        return assignerWithTimeWindows;
    }

    public PravegaSourceBuilder<T> withEnvironmentAndParameter(@Nullable ParameterTool params) {
        this.pravegaClientConfig.putAll(getPropertiesFromEnvironmentAndCommand(params));
        return this;
    }

    public PravegaSourceBuilder<T> withPravegaClientConfig(Properties pravegaClientConfig) {
        Preconditions.checkNotNull(pravegaClientConfig, "pravegaClientConfig");
        this.pravegaClientConfig.putAll(pravegaClientConfig);
        return this;
    }

    /**
     * enable/disable pravega reader metrics (default: enabled).
     *
     * @param enable boolean
     * @return A builder to configure and create a reader.
     */
    public PravegaSourceBuilder<T> enableMetrics(boolean enable) {
        this.enableMetrics = enable;
        return this;
    }

    /**
     * Configures the reader group scope for synchronization purposes.
     * <p>
     * The default value is taken from the {@link PravegaConfig} {@code defaultScope} property.
     *
     * @param scope the scope name.
     * @return A builder to configure and create a streaming reader.
     */
    public PravegaSourceBuilder<T> withReaderGroupScope(String scope) {
        this.readerGroupScope = Preconditions.checkNotNull(scope);
        return this;
    }

    /**
     * Configures the reader group name.
     *
     * @param readerGroupName the reader group name.
     * @return A builder to configure and create a streaming reader.
     */
    public PravegaSourceBuilder<T> withReaderGroupName(String readerGroupName) {
        this.readerGroupName = Preconditions.checkNotNull(readerGroupName);
        return this;
    }

    /**
     * Sets the group refresh time, with a default of 1 second.
     *
     * @param groupRefreshTime The group refresh time
     * @return A builder to configure and create a streaming reader.
     */
    public PravegaSourceBuilder<T> withReaderGroupRefreshTime(Time groupRefreshTime) {
        this.readerGroupRefreshTime = groupRefreshTime;
        return this;
    }

    /**
     * Sets the timeout for initiating a checkpoint in Pravega.
     *
     * @param checkpointInitiateTimeout The timeout
     * @return A builder to configure and create a streaming reader.
     */
    public PravegaSourceBuilder<T> withCheckpointInitiateTimeout(Time checkpointInitiateTimeout) {
        Preconditions.checkArgument(checkpointInitiateTimeout.getSize() > 0, "timeout must be > 0");
        this.checkpointInitiateTimeout = checkpointInitiateTimeout;
        return this;
    }

    /**
     * Sets the timeout for the call to read events from Pravega. After the timeout
     * expires (without an event being returned), another call will be made.
     *
     * @param eventReadTimeout The timeout
     * @return A builder to configure and create a streaming reader.
     */
    public PravegaSourceBuilder<T> withEventReadTimeout(Time eventReadTimeout) {
        Preconditions.checkArgument(eventReadTimeout.getSize() > 0, "timeout must be > 0");
        this.eventReadTimeout = eventReadTimeout;
        return this;
    }

    /**
     * Configures the maximum outstanding checkpoint requests to Pravega (default=3).
     * Upon requesting more checkpoints than the specified maximum,
     * (say a checkpoint request times out on the ReaderCheckpointHook but Pravega is still working on it),
     * this configurations allows Pravega to limit any further checkpoint request being made to the ReaderGroup.
     * This configuration is particularly relevant when multiple checkpoint requests need to be honored (e.g., frequent savepoint requests being triggered concurrently).
     *
     * @param maxOutstandingCheckpointRequest maximum outstanding checkpoint request.
     * @return A builder to configure and create a streaming reader.
     */
    public PravegaSourceBuilder<T> withMaxOutstandingCheckpointRequest(int maxOutstandingCheckpointRequest) {
        this.maxOutstandingCheckpointRequest = maxOutstandingCheckpointRequest;
        return this;
    }

    /**
     * Add a stream to be read by the source, from the earliest available position in the stream.
     *
     * @param streamSpec the unqualified or qualified name of the stream.
     * @return A builder to configure and create a reader.
     */
    public PravegaSourceBuilder<T> forStream(final String streamSpec) {
        return forStream(streamSpec, StreamCut.UNBOUNDED, StreamCut.UNBOUNDED);
    }

    /**
     * Add a stream to be read by the source, from the given start position in the stream.
     *
     * @param streamSpec the unqualified or qualified name of the stream.
     * @param startStreamCut Start {@link StreamCut}
     * @return A builder to configure and create a reader.
     */
    public PravegaSourceBuilder<T> forStream(final String streamSpec, final StreamCut startStreamCut) {
        return forStream(streamSpec, startStreamCut, StreamCut.UNBOUNDED);
    }

    /**
     * Add a stream to be read by the source, from the given start position in the stream.
     *
     * @param streamSpec the unqualified or qualified name of the stream.
     * @param startStreamCut Start {@link StreamCut}
     * @param endStreamCut End {@link StreamCut}
     * @return A builder to configure and create a reader.
     */
    public PravegaSourceBuilder<T> forStream(final String streamSpec, final StreamCut startStreamCut, final StreamCut endStreamCut) {
        Preconditions.checkNotNull(streamSpec, "streamSpec");
        Preconditions.checkNotNull(startStreamCut, "from");
        Preconditions.checkNotNull(endStreamCut, "to");
        streams.add(Triple.of(streamSpec, startStreamCut, endStreamCut));
        return this;
    }

    /**
     * Add a stream to be read by the source, from the earliest available position in the stream.
     *
     * @param stream Stream.
     * @return A builder to configure and create a reader.
     */
    public PravegaSourceBuilder<T> forStream(final Stream stream) {
        return forStream(stream, StreamCut.UNBOUNDED, StreamCut.UNBOUNDED);
    }

    /**
     * Add a stream to be read by the source, from the given start position in the stream.
     *
     * @param stream Stream.
     * @param startStreamCut Start {@link StreamCut}
     * @return A builder to configure and create a reader.
     */
    public PravegaSourceBuilder<T> forStream(final Stream stream, final StreamCut startStreamCut) {
        return forStream(stream, startStreamCut, StreamCut.UNBOUNDED);
    }

    /**
     * Add a stream to be read by the source, from the given start position in the stream to the given end position.
     *
     * @param stream Stream.
     * @param startStreamCut Start {@link StreamCut}
     * @param endStreamCut End {@link StreamCut}
     * @return A builder to configure and create a reader.
     */
    public PravegaSourceBuilder<T> forStream(final Stream stream, final StreamCut startStreamCut, final StreamCut endStreamCut) {
        Preconditions.checkNotNull(stream, "streamSpec");
        Preconditions.checkNotNull(startStreamCut, "from");
        Preconditions.checkNotNull(endStreamCut, "to");
        streams.add(Triple.of(stream.getScopedName(), startStreamCut, endStreamCut));
        return this;
    }

    /**
     * Resolves the given stream name.
     *
     * The scope name is resolved in the following order:
     * 1. from the stream name (if fully-qualified)
     * 2. from the program argument {@code --scope} (if program arguments were provided to the {@link PravegaConfig})
     * 3. from the system property {@code pravega.scope}
     * 4. from the system environment variable {@code PRAVEGA_SCOPE}
     *
     * @param streamSpec a qualified or unqualified stream name
     * @return a fully-qualified stream name
     * @throws IllegalStateException if an unqualified stream name is supplied but the scope is not configured.
     */
    public Stream resolve(String streamSpec) {
        Preconditions.checkNotNull(streamSpec, "streamSpec");
        String[] split = streamSpec.split("/", 2);
        if (split.length == 1) {
            // unqualified
            Preconditions.checkState(pravegaClientConfig.containsKey(PravegaOptions.DEFAULT_SCOPE), "The default scope is not configured.");
            return Stream.of(pravegaClientConfig.getProperty(PravegaOptions.DEFAULT_SCOPE), split[0]);
        } else {
            // qualified
            assert split.length == 2;
            return Stream.of(split[0], split[1]);
        }
    }

    /**
     * Build reader group configuration from streams and defaultScope.
     *
     * @return rgConfig, rgScope, and rgName.
     */
    public Triple<ReaderGroupConfig, String, String> buildReaderGroupInfo() {
        // rgConfig
        ReaderGroupConfig.ReaderGroupConfigBuilder rgConfigBuilder = ReaderGroupConfig
                .builder()
                .maxOutstandingCheckpointRequest(maxOutstandingCheckpointRequest)
                .disableAutomaticCheckpoints();
        if (readerGroupRefreshTime != null) {
            rgConfigBuilder.groupRefreshTimeMillis(readerGroupRefreshTime.toMilliseconds());
        }
        Preconditions.checkState(!streams.isEmpty(), "At least one stream must be supplied.");
        streams.forEach(s -> rgConfigBuilder.stream(resolve(s.getLeft()), s.getMiddle(), s.getRight()));
        final ReaderGroupConfig rgConfig = rgConfigBuilder.build();

        // rgScope
        final String rgScope = Optional.ofNullable(readerGroupScope).orElseGet(() -> {
            Preconditions.checkState(pravegaClientConfig.containsKey(PravegaOptions.DEFAULT_SCOPE),  "A reader group scope or default scope must be configured");
            return pravegaClientConfig.getProperty(PravegaOptions.DEFAULT_SCOPE);
        });

        // rgName
        final String rgName = Optional.ofNullable(readerGroupName).orElseGet(FlinkPravegaUtils::generateRandomReaderGroupName);
        return Triple.of(rgConfig, rgScope, rgName);
    }

    /**
     * Builds a {@link PravegaSource} based on the configuration.
     *
     * @throws IllegalStateException if the configuration is invalid.
     * @return an uninitiailized reader as a source function.
     */
    private PravegaSource<T> buildSource() {
        // get rgConfig, rgScope, and rgName from streams and defaultScope.
        Triple<ReaderGroupConfig, String, String> readerGroupInfo = buildReaderGroupInfo();

        return new PravegaSource<>(
                buildClientConfigFromProperties(this.pravegaClientConfig),
                readerGroupInfo.getLeft(),
                readerGroupInfo.getMiddle(),
                readerGroupInfo.getRight(),
                getDeserializationSchema(),
                this.eventReadTimeout,
                this.checkpointInitiateTimeout,
                this.enableMetrics);
    }

    /**
     * Builds a {@link PravegaSource}.
     */
    public PravegaSource<T> build() {
        PravegaSource<T> source = buildSource();
        return source;
    }
}
