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
import io.pravega.client.stream.Serializer;
import io.pravega.connectors.flink.dynamic.table.FlinkPravegaDynamicTableSource.ReadableMetadata;
import io.pravega.connectors.flink.serialization.PravegaDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import java.util.List;

public class FlinkPravegaDynamicDeserializationSchema extends PravegaDeserializationSchema<RowData> {
    // the produced RowData type, may have metadata keys in it
    private final TypeInformation<RowData> typeInfo;

    // metadata keys that the rowData have and is a subset of ReadableMetadata
    private final List<ReadableMetadata> metadataKeys;

    public FlinkPravegaDynamicDeserializationSchema(
            TypeInformation<RowData> typeInfo,
            Serializer<RowData> serializer,
            List<ReadableMetadata> metadataKeys) {
        super(typeInfo, serializer);
        this.typeInfo = typeInfo;
        this.metadataKeys = metadataKeys;
    }

    @Override
    public RowData extractEvent(EventRead<RowData> eventRead) {
        RowData rowData = eventRead.getEvent();
        if (metadataKeys.size() == 0) {
            return rowData;
        }

        // use GenericRowData to manipulate rowData's field
        final GenericRowData physicalRow = (GenericRowData) rowData;
        final GenericRowData producedRow = new GenericRowData(physicalRow.getRowKind(), typeInfo.getArity());

        // set the physical(original) field
        int pos = 0, physicalArity = typeInfo.getArity() - metadataKeys.size();
        for (; pos < physicalArity; pos++) {
            producedRow.setField(pos, physicalRow.getField(pos));
        }

        // set the metadata field after the physical field, no effect if the key is not supported
        for (; pos < typeInfo.getArity(); pos++) {
            ReadableMetadata metadataKey = metadataKeys.get(pos - physicalArity);
            if (ReadableMetadata.EVENT_POINTER == metadataKey) {
                producedRow.setField(pos, eventRead.getEventPointer().toBytes().array());
            }
        }

        return producedRow;
    }
}
