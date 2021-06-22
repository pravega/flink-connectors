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

import io.pravega.client.stream.EventRead;
import io.pravega.connectors.flink.dynamic.table.FlinkPravegaDynamicTableSource.ReadableMetadata;
import io.pravega.connectors.flink.serialization.PravegaDeserializationSchema;
import io.pravega.connectors.flink.serialization.SupportsReadingMetadata;
import io.pravega.connectors.flink.util.FlinkPravegaUtils.FlinkDeserializer;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;

public class FlinkPravegaDynamicDeserializationSchema
        extends PravegaDeserializationSchema<RowData>
        implements SupportsReadingMetadata<RowData> {
    // nested schema
    private final DeserializationSchema<RowData> nestedSchema;

    // the custom collector that adds metadata to the row
    private final OutputCollector outputCollector;

    public FlinkPravegaDynamicDeserializationSchema(
            TypeInformation<RowData> typeInfo,
            int physicalArity,
            List<String> metadataKeys,
            DeserializationSchema<RowData> nestedSchema) {
        super(typeInfo, new FlinkDeserializer<>(nestedSchema));
        this.nestedSchema = nestedSchema;
        this.outputCollector = new OutputCollector(metadataKeys, physicalArity);
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        this.nestedSchema.open(context);
    }

    @Override
    public void deserialize(byte[] message,
                            EventRead<ByteBuffer> eventReadByteBuffer,
                            Collector<RowData> out) throws IOException {
        this.outputCollector.eventReadByteBuffer = eventReadByteBuffer;
        this.outputCollector.out = out;

        this.deserialize(message, this.outputCollector);
    }


    private static final class OutputCollector implements Collector<RowData>, Serializable {
        private static final long serialVersionUID = 1L;

        // the original collector which need both original and metadata keys
        public Collector<RowData> out;

        // where we get event pointer from
        public EventRead<ByteBuffer> eventReadByteBuffer;

        // metadata keys that the rowData have and is a subset of ReadableMetadata
        private final List<String> metadataKeys;

        // source datatype arity without metadata
        private final int physicalArity;

        private OutputCollector(List<String> metadataKeys, int physicalArity) {
            this.metadataKeys = metadataKeys;
            this.physicalArity = physicalArity;
        }

        @Override
        public void collect(RowData record) {
            if (this.metadataKeys.size() == 0 || record != null) {
                record = enrichWithMetadata(record, eventReadByteBuffer);
            }

            out.collect(record);
        }

        @Override
        public void close() {
            // nothing to do
        }

        public RowData enrichWithMetadata(RowData rowData, EventRead<ByteBuffer> eventReadByteBuffer) {
            // use GenericRowData to manipulate rowData's field
            final GenericRowData producedRow = new GenericRowData(rowData.getRowKind(), physicalArity + metadataKeys.size());

            // set the physical(original) field
            final GenericRowData physicalRow = (GenericRowData) rowData;
            int pos = 0;
            for (; pos < physicalArity; pos++) {
                producedRow.setField(pos, physicalRow.getField(pos));
            }

            // set the virtual(metadata) field after the physical field, no effect if the key is not supported
            for (; pos < physicalArity + metadataKeys.size(); pos++) {
                String metadataKey = metadataKeys.get(pos - physicalArity);
                if (ReadableMetadata.EVENT_POINTER.key.equals(metadataKey)) {
                    producedRow.setField(pos, eventReadByteBuffer.getEventPointer().toBytes().array());
                }
            }

            return producedRow;
        }
    }
}
