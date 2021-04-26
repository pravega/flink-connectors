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
import io.pravega.connectors.flink.serialization.PravegaDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import java.nio.ByteBuffer;
import java.util.List;

public class FlinkPravegaDynamicDeserializationSchema extends PravegaDeserializationSchema<RowData> {
    private final TypeInformation<RowData> typeInfo;

    // private final DeserializationSchema<RowData> valueDeserialization;
    private final Serializer<RowData> serializer;

    private final List<String> metadataKeys;

    public FlinkPravegaDynamicDeserializationSchema(
            TypeInformation<RowData> typeInfo,
            Serializer<RowData> serializer,
            List<String> metadataKeys) {
        super(typeInfo, serializer);
        this.serializer = serializer;
        this.typeInfo = typeInfo;
        this.metadataKeys = metadataKeys;
    }

    @Override
    public RowData extractEvent(EventRead<RowData> eventRead) {
        RowData rowData = eventRead.getEvent();
        if (metadataKeys.size() == 0) {
            return rowData;
        }

        final ByteBuffer eventPointer = eventRead.getEventPointer().toBytes();
        rowData = serializer.deserialize(eventPointer);

        final GenericRowData physicalRow = (GenericRowData) rowData;
        final GenericRowData producedRow = new GenericRowData(physicalRow.getRowKind(), typeInfo.getArity());

        for (int dataPos = 0; dataPos < typeInfo.getArity() - metadataKeys.size(); dataPos++) {
            producedRow.setField(dataPos, physicalRow.getField(dataPos));
        }

        for (int metadataPos = typeInfo.getArity() - metadataKeys.size(); metadataPos < typeInfo.getArity(); metadataPos++) {
            producedRow.setField(metadataPos, physicalRow.getField(metadataPos));
        }

        return producedRow;
    }
}
