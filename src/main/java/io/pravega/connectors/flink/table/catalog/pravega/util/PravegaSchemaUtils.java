/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.flink.table.catalog.pravega.util;

import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import io.pravega.schemaregistry.serializer.json.schemas.JSONSchema;
import org.apache.avro.Schema;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.annotation.Internal;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.table.types.utils.TypeConversions;

@Internal
public class PravegaSchemaUtils {

    private PravegaSchemaUtils() {
        // private
    }

    public static TableSchema schemaInfoToTableSchema(SchemaInfo schemaInfo) {

        SerializationFormat format = schemaInfo.getSerializationFormat();
        String schemaString;
        DataType dataType;

        switch (format) {
            case Json:
                JSONSchema jsonSchema = JSONSchema.from(schemaInfo);
                schemaString = jsonSchema.getSchemaString();
                dataType = TypeConversions.fromLegacyInfoToDataType(JsonRowSchemaConverter.convert(schemaString));
                break;
            case Avro:
                AvroSchema avroSchema = AvroSchema.from(schemaInfo);

                schemaString = avroSchema.getSchema().toString();
                dataType = TypeConversions.fromLegacyInfoToDataType(AvroSchemaConverter.convertToTypeInfo(schemaString));
                break;

            default:
                throw new NotImplementedException("Not supporting serialization format");
        }

        return DataTypeUtils.expandCompositeTypeToSchema(dataType);
    }

    public static SchemaInfo tableSchemaToSchemaInfo(TableSchema tableSchema) {
        // only support avro format for now
        Schema schema = AvroSchemaConverter.convertToSchema(tableSchema.toRowDataType().getLogicalType());
        AvroSchema avroSchema = AvroSchema.of(schema);
        return avroSchema.getSchemaInfo();
    }
}
