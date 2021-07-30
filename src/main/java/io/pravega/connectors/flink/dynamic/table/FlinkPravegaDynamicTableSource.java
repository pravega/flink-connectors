/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.connectors.flink.dynamic.table;

import io.pravega.connectors.flink.FlinkPravegaInputFormat;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.util.StreamWithBoundaries;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class FlinkPravegaDynamicTableSource implements ScanTableSource {

    // Source produced data type
    private final DataType producedDataType;

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
     * @param producedDataType                source produced data type
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
    public FlinkPravegaDynamicTableSource(DataType producedDataType,
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
        this.producedDataType = Preconditions.checkNotNull(
                producedDataType, "Produced data type must not be null.");
        this.decodingFormat = Preconditions.checkNotNull(
                decodingFormat, "Decoding format must not be null.");
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
        if (isStreamingReader) {
            FlinkPravegaReader.Builder<RowData> readerBuilder = FlinkPravegaReader.<RowData>builder()
                    .withPravegaConfig(pravegaConfig)
                    .withDeserializationSchema(decodingFormat.createRuntimeDecoder(runtimeProviderContext, producedDataType))
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
                    .withDeserializationSchema(decodingFormat.createRuntimeDecoder(runtimeProviderContext, producedDataType));

            for (StreamWithBoundaries stream : streams) {
                inputFormatBuilder.forStream(stream.getStream(), stream.getFrom(), stream.getTo());
            }

            return InputFormatProvider.of(inputFormatBuilder.build());
        }
    }

    @Override
    public DynamicTableSource copy() {
        return new FlinkPravegaDynamicTableSource(
                this.producedDataType,
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
                decodingFormat.equals(that.decodingFormat) &&
                Objects.equals(readerGroupName, that.readerGroupName) &&
                pravegaConfig.equals(that.pravegaConfig) &&
                streams.equals(that.streams) &&
                Objects.equals(uid, that.uid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                producedDataType,
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
                isBounded);
    }
}
