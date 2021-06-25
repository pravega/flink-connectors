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
import io.pravega.connectors.flink.serialization.PravegaDeserializationSchemaWithMetadata;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static io.pravega.connectors.flink.util.FlinkPravegaUtils.byteBufferToArray;

public class FlinkPravegaDynamicDeserializationSchema extends PravegaDeserializationSchemaWithMetadata<RowData> {
    private final TypeInformation<RowData> typeInfo;

    private final DeserializationSchema<RowData> nestedSchema;

    // metadata keys that the rowData have and is a subset of ReadableMetadata
    private final List<String> metadataKeys;

    // source datatype arity without metadata
    private final int physicalArity;

    public FlinkPravegaDynamicDeserializationSchema(
            TypeInformation<RowData> typeInfo,
            int physicalArity,
            List<String> metadataKeys,
            DeserializationSchema<RowData> nestedSchema) {
        this.typeInfo = typeInfo;
        this.nestedSchema = nestedSchema;
        this.metadataKeys = metadataKeys;
        this.physicalArity = physicalArity;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        this.nestedSchema.open(context);
    }

    @Override
    public RowData deserialize(byte[] message, EventRead<ByteBuffer> eventRead) throws IOException {
        RowData rowData = this.nestedSchema.deserialize(message);
        if (rowData != null && metadataKeys.size() > 0) {
            rowData = enrichWithMetadata(rowData, eventRead);
        }
        return rowData;
    }

    public RowData enrichWithMetadata(RowData rowData, EventRead<ByteBuffer> eventRead) {
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
                producedRow.setField(pos, byteBufferToArray(eventRead.getEventPointer().toBytes()));
            }
        }

        return producedRow;
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return this.typeInfo;
    }
}
