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

import io.pravega.connectors.flink.AbstractStreamingReaderBuilder;
import io.pravega.connectors.flink.watermark.AssignerWithTimeWindows;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import java.io.IOException;

/**
 *The @builder class for {@link PravegaSource} to make it easier for the users to construct a {@link
 *  PravegaSource}.
 *
 * <p>The following example shows the minimum setup to create a PravegaSource that reads the Integer
 * values from a Pravega Stream.
 *
 * <pre>{@code
 * PravegaSource<Integer> pravegaSource = PravegaSource.<Integer>builder()
 *                     .forStream(streamName)
 *                     .withPravegaConfig(pravegaConfig)
 *                     .withReaderGroupName("flink-reader")
 *                     .withDeserializationSchema(new IntegerDeserializationSchema())
 *                     .build();
 * }</pre>
 *
 * <p>The stream name, Pravega client configuration, the readerGroup name and the event deserialization schema
 * are required fields that must be set.
 *
 * <p>Check the Java docs of each individual methods to learn more about the settings to build a
 * PravegaSource.
 */
@PublicEvolving
public class PravegaSourceBuilder<T> extends AbstractStreamingReaderBuilder<T, PravegaSourceBuilder<T>> {

    private DeserializationSchema<T> deserializationSchema;
    private SerializedValue<AssignerWithTimeWindows<T>> assignerWithTimeWindows;

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

    @Override
    protected DeserializationSchema<T> getDeserializationSchema() {
        Preconditions.checkState(deserializationSchema != null, "Deserialization schema must not be null.");
        return deserializationSchema;
    }

    @Override
    protected SerializedValue<AssignerWithTimeWindows<T>> getAssignerWithTimeWindows() {
        return assignerWithTimeWindows;
    }

    /**
     * Builds a {@link PravegaSource} based on the configuration.
     *
     * @throws IllegalStateException if the configuration is invalid.
     * @return an uninitialized reader as a source function.
     */
    public PravegaSource<T> build() {
        PravegaSourceBuilder.ReaderGroupInfo readerGroupInfo = buildReaderGroupInfo();
        return new PravegaSource<>(
                getPravegaConfig().getClientConfig(),
                readerGroupInfo.getReaderGroupConfig(),
                readerGroupInfo.getReaderGroupScope(),
                readerGroupInfo.getReaderGroupName(),
                getDeserializationSchema(),
                this.eventReadTimeout,
                this.checkpointInitiateTimeout,
                isMetricsEnabled());
    }
}
