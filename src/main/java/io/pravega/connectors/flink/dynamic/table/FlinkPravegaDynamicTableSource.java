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
package io.pravega.connectors.flink.dynamic.table;

import io.pravega.connectors.flink.FlinkPravegaInputFormat;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.util.StreamWithBoundaries;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FlinkPravegaDynamicTableSource implements ScanTableSource, SupportsReadingMetadata {

    private static final String FORMAT_METADATA_PREFIX = "from_format.";

    // Source produced data type
    protected DataType producedDataType;

    // Data type to configure the format
    private final DataType physicalDataType;

    // Metadata that is appended at the end of a physical source row
    private List<String> metadataKeys;

    // Scan format for decoding records from Pravega
    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

    // The reader group name to coordinate the parallel readers. This should be unique for a Flink job.
    @Nullable
    private final String readerGroupName;

    // Pravega connection configuration
    private final PravegaConfig pravegaConfig;

    // Pravega source streams with start and end streamcuts
    private final List<StreamWithBoundaries> streams;

    // Refresh interval for reader group
    private final long readerGroupRefreshTimeMillis;

    // Timeout for call that initiates the Pravega checkpoint
    private final long checkpointInitiateTimeoutMillis;

    // Timeout for event read call
    private final long eventReadTimeoutMillis;

    // Maximum outstanding Pravega checkpoint requests
    private final int maxOutstandingCheckpointRequest;

    // Uid of the table source to identify the checkpoint state
    @Nullable
    private final String uid;

    // Flag to determine streaming or batch read
    private final boolean isStreamingReader;

    // Flag to determine if the source stream is bounded
    private final boolean isBounded;

    /**
     * Creates a Pravega {@link DynamicTableSource}.
     * @param physicalDataType                source produced data type
     * @param decodingFormat                  scan format for decoding records from Pravega
     * @param readerGroupName                 the reader group name
     * @param pravegaConfig                   Pravega connection configuration
     * @param streams                         list of Pravega source streams with start and end streamcuts
     * @param uid                             uid of the table source
     * @param readerGroupRefreshTimeMillis    refresh interval for reader group
     * @param checkpointInitiateTimeoutMillis timeout for call that initiates the Pravega checkpoint
     * @param eventReadTimeoutMillis          timeout for event read call
     * @param maxOutstandingCheckpointRequest maximum outstanding Pravega checkpoint requests
     * @param isStreamingReader               flag to determine streaming or batch read
     * @param isBounded                       flag to determine if the source stream is bounded
     */
    public FlinkPravegaDynamicTableSource(DataType physicalDataType,
                                          DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
                                          String readerGroupName,
                                          PravegaConfig pravegaConfig,
                                          List<StreamWithBoundaries> streams,
                                          long readerGroupRefreshTimeMillis,
                                          long checkpointInitiateTimeoutMillis,
                                          long eventReadTimeoutMillis,
                                          int maxOutstandingCheckpointRequest,
                                          String uid,
                                          boolean isStreamingReader,
                                          boolean isBounded) {
        this(
                physicalDataType,
                // producedDataType should be the same as physicalDataType on initialization
                // and will be updated on `applyReadableMetadata`
                physicalDataType,
                // metadataKeys will be empty on initialization and will be updated on `applyReadableMetadata`
                Collections.emptyList(),
                decodingFormat,
                readerGroupName,
                pravegaConfig,
                streams,
                readerGroupRefreshTimeMillis,
                checkpointInitiateTimeoutMillis,
                eventReadTimeoutMillis,
                maxOutstandingCheckpointRequest,
                uid,
                isStreamingReader,
                isBounded
        );
    }

    FlinkPravegaDynamicTableSource(DataType physicalDataType,
                                   DataType producedDataType,
                                   List<String> metadataKeys,
                                   DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
                                   String readerGroupName,
                                   PravegaConfig pravegaConfig,
                                   List<StreamWithBoundaries> streams,
                                   long readerGroupRefreshTimeMillis,
                                   long checkpointInitiateTimeoutMillis,
                                   long eventReadTimeoutMillis,
                                   int maxOutstandingCheckpointRequest,
                                   String uid,
                                   boolean isStreamingReader,
                                   boolean isBounded) {
        this.physicalDataType = Preconditions.checkNotNull(
                physicalDataType, "Physical data type must not be null.");
        this.producedDataType = Preconditions.checkNotNull(
                producedDataType, "Produced data type must not be null.");
        this.decodingFormat = Preconditions.checkNotNull(
                decodingFormat, "Decoding format must not be null.");
        this.metadataKeys = Preconditions.checkNotNull(
                metadataKeys, "Metadata Keys must not be null.");
        this.readerGroupName = readerGroupName;
        this.pravegaConfig = Preconditions.checkNotNull(
                pravegaConfig, "Pravega config must not be null.");
        this.streams = Preconditions.checkNotNull(
                streams, "Source streams must not be null.");
        this.readerGroupRefreshTimeMillis = readerGroupRefreshTimeMillis;
        this.checkpointInitiateTimeoutMillis = checkpointInitiateTimeoutMillis;
        this.eventReadTimeoutMillis = eventReadTimeoutMillis;
        this.maxOutstandingCheckpointRequest = maxOutstandingCheckpointRequest;
        this.uid = uid;
        this.isStreamingReader = isStreamingReader;
        this.isBounded = isBounded;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return this.decodingFormat.getChangelogMode();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        // create a PravegaDeserializationSchema that will expose metadata to the row
        final FlinkPravegaDynamicDeserializationSchema deserializationSchema
                = new FlinkPravegaDynamicDeserializationSchema(
                runtimeProviderContext.createTypeInformation(producedDataType),
                producedDataType.getChildren().size() - metadataKeys.size(),
                metadataKeys,
                decodingFormat.createRuntimeDecoder(runtimeProviderContext, physicalDataType));

        if (isStreamingReader) {
            FlinkPravegaReader.Builder<RowData> readerBuilder = FlinkPravegaReader.<RowData>builder()
                    .withPravegaConfig(pravegaConfig)
                    .withDeserializationSchema(deserializationSchema)
                    .withReaderGroupRefreshTime(Time.milliseconds(readerGroupRefreshTimeMillis))
                    .withCheckpointInitiateTimeout(Time.milliseconds(checkpointInitiateTimeoutMillis))
                    .withEventReadTimeout(Time.milliseconds(eventReadTimeoutMillis))
                    .withMaxOutstandingCheckpointRequest(maxOutstandingCheckpointRequest);
            Optional.ofNullable(readerGroupName).ifPresent(readerBuilder::withReaderGroupName);

            for (StreamWithBoundaries stream : streams) {
                readerBuilder.forStream(stream.getStream(), stream.getFrom(), stream.getTo());
            }

            readerBuilder.uid(uid == null ? readerBuilder.generateUid() : uid);

            return SourceFunctionProvider.of(readerBuilder.build(), isBounded);
        } else {
            FlinkPravegaInputFormat.Builder<RowData> inputFormatBuilder =
                    FlinkPravegaInputFormat.<RowData>builder()
                            .withPravegaConfig(pravegaConfig)
                            .withDeserializationSchema(deserializationSchema);

            for (StreamWithBoundaries stream : streams) {
                inputFormatBuilder.forStream(stream.getStream(), stream.getFrom(), stream.getTo());
            }

            return InputFormatProvider.of(inputFormatBuilder.build());
        }
    }

    @Override
    public DynamicTableSource copy() {
        return new FlinkPravegaDynamicTableSource(
                this.physicalDataType,
                this.producedDataType,
                this.metadataKeys,
                this.decodingFormat,
                this.readerGroupName,
                this.pravegaConfig,
                this.streams,
                this.readerGroupRefreshTimeMillis,
                this.checkpointInitiateTimeoutMillis,
                this.eventReadTimeoutMillis,
                this.maxOutstandingCheckpointRequest,
                this.uid,
                this.isStreamingReader,
                this.isBounded);
    }

    @Override
    public String asSummaryString() {
        return "Pravega";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final FlinkPravegaDynamicTableSource that = (FlinkPravegaDynamicTableSource) o;
        return readerGroupRefreshTimeMillis == that.readerGroupRefreshTimeMillis &&
                checkpointInitiateTimeoutMillis == that.checkpointInitiateTimeoutMillis &&
                eventReadTimeoutMillis == that.eventReadTimeoutMillis &&
                maxOutstandingCheckpointRequest == that.maxOutstandingCheckpointRequest &&
                isStreamingReader == that.isStreamingReader &&
                isBounded == that.isBounded &&
                producedDataType.equals(that.producedDataType) &&
                physicalDataType.equals(that.physicalDataType) &&
                decodingFormat.equals(that.decodingFormat) &&
                metadataKeys.equals(that.metadataKeys) &&
                Objects.equals(readerGroupName, that.readerGroupName) &&
                pravegaConfig.equals(that.pravegaConfig) &&
                streams.equals(that.streams) &&
                Objects.equals(uid, that.uid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                producedDataType,
                physicalDataType,
                decodingFormat,
                metadataKeys,
                readerGroupName,
                pravegaConfig,
                streams,
                readerGroupRefreshTimeMillis,
                checkpointInitiateTimeoutMillis,
                eventReadTimeoutMillis,
                maxOutstandingCheckpointRequest,
                uid,
                isStreamingReader,
                isBounded);
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        final Map<String, DataType> metadataMap = new LinkedHashMap<>();

        // according to convention, the order of the final row must be
        // PHYSICAL + FORMAT METADATA + CONNECTOR METADATA
        // where the format metadata has highest precedence

        // add value format metadata with prefix
        this.decodingFormat
                .listReadableMetadata()
                .forEach((key, value) -> metadataMap.put(FORMAT_METADATA_PREFIX + key, value));

        // add connector metadata
        Stream.of(ReadableMetadata.values())
                .forEachOrdered(m -> metadataMap.put(m.key, m.dataType));

        return metadataMap;
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        // separate connector and format metadata
        Map<Boolean, List<String>> partitions = metadataKeys
                .stream()
                .collect(Collectors.partitioningBy(key -> key.startsWith(FORMAT_METADATA_PREFIX)));

        // push down format metadata
        final Map<String, DataType> formatMetadata = this.decodingFormat.listReadableMetadata();
        if (formatMetadata.size() > 0) {
            this.decodingFormat
                    .applyReadableMetadata(partitions.get(true)
                            .stream()
                            .map(k -> k.substring(FORMAT_METADATA_PREFIX.length()))
                            .collect(Collectors.toList()));
        }

        this.metadataKeys = partitions.get(false);
        this.producedDataType = producedDataType;
    }

    enum ReadableMetadata {
        EVENT_POINTER(
                "event_pointer",
                DataTypes.BYTES().notNull()
        );

        final String key;

        final DataType dataType;

        ReadableMetadata(String key, DataType dataType) {
            this.key = key;
            this.dataType = dataType;
        }
    }
}
