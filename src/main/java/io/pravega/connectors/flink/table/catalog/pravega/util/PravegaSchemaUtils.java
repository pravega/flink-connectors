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
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.annotation.Internal;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.table.types.utils.TypeConversions;

import java.util.List;

@Slf4j
@Internal
public class PravegaSchemaUtils {

    private PravegaSchemaUtils() {
        // private
    }

    public static ResolvedSchema schemaInfoToResolvedSchema(SchemaInfo schemaInfo) {

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

    public static SchemaInfo tableSchemaToSchemaInfo(TableSchema tableSchema, SerializationFormat serializationFormat) {
        switch (serializationFormat) {
            case Avro:
                Schema schema = AvroSchemaConverter.convertToSchema(tableSchema.toRowDataType().getLogicalType());
                AvroSchema avroSchema = AvroSchema.of(schema);
                return avroSchema.getSchemaInfo();
            case Json:
                LogicalType logicalType = tableSchema.toRowDataType().getLogicalType();
                String schemaString = convertToJsonSchemaString(logicalType);
                JSONSchema<JsonNode> jsonSchema = JSONSchema.of("", schemaString, JsonNode.class);
                return jsonSchema.getSchemaInfo();
            default:
                throw new NotImplementedException("Not supporting serialization format");
        }
    }

    /**
     * Converts Flink SQL {@link LogicalType} (can be nested) into a Json Schema String.
     * <p>
     * @param logicalType logical type
     * @return String matching this logical type.
     */
    public static String convertToJsonSchemaString(LogicalType logicalType) {
        StringBuilder sb = new StringBuilder();
        switch (logicalType.getTypeRoot()) {
            case NULL:
                sb.append("null");
                break;
            case BOOLEAN:
                sb.append("boolean");
                break;
            case CHAR:
            case VARCHAR:
                sb.append("string");
                break;
            case BINARY:
            case VARBINARY:
                sb.append("string").append("\", \"").append("contentEncoding")
                        .append("\": \"").append("base64");
                break;
            case DECIMAL:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case INTERVAL_DAY_TIME:
            case INTERVAL_YEAR_MONTH:
                sb.append("number");
                break;
            case DATE:
                sb.append("string").append("\", \"").append("format")
                        .append("\": \"").append("date");
                break;
            case TIME_WITHOUT_TIME_ZONE:
                sb.append("string").append("\", \"").append("format")
                        .append("\": \"").append("time");
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                sb.append("string").append("\", \"").append("format")
                        .append("\": \"").append("date-time");
                break;
            case ARRAY:
                sb.append("array");
                break;
            case MULTISET:
            case MAP:
                sb.append("object");
                break;
            case ROW:
                RowType rowType = (RowType) logicalType;
                List<String> fieldNames = rowType.getFieldNames();
                sb.append("{").append("\"title\": ").append("\"Json Schema\", ").
                        append("\"type\": ").append("\"object\", ").
                        append("\"properties\": { ");
                for (int i = 0; i < rowType.getFieldCount(); i++) {
                    String fieldName = fieldNames.get(i);
                    String fieldType = convertToJsonSchemaString(rowType.getTypeAt(i));
                    sb.append("\"").append(fieldName).append("\": {").
                            append("\"type\": \"").append(fieldType).append("\"},");
                }
                sb.deleteCharAt(sb.length() - 1).append("}}");
                break;
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + logicalType);
        }
        return sb.toString();
    }
}
