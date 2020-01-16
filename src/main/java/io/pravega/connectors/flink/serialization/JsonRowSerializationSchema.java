/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.flink.serialization;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Serialization schema that serializes an object into a JSON bytes.
 *
 * <p>Serializes the input {@link Row} object into a JSON string and
 * converts it into <code>byte[]</code>.
 *
 * <p>Result <code>byte[]</code> messages can be deserialized using
 * {@link JsonRowDeserializationSchema}.
 *
 * @deprecated Please use {@link org.apache.flink.formats.json.JsonRowSerializationSchema} from flink-json module
 */
@Deprecated
public class JsonRowSerializationSchema implements SerializationSchema<Row> {

    /**
     * Object MAPPER that is used to create output JSON objects.
     */
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Fields names in the input Row object.
     */
    private final String[] fieldNames;

    /**
     * Creates a JSON serialization schema for the given fields and types.
     *
     * @param fieldNames Names of JSON fields to parse.
     */
    public JsonRowSerializationSchema(String[] fieldNames) {
        this.fieldNames = Preconditions.checkNotNull(fieldNames);
    }

    @Override
    public byte[] serialize(Row row) {
        if (row.getArity() != fieldNames.length) {
            throw new IllegalStateException(String.format(
                    "Number of elements in the row %s is different from number of field names: %d", row, fieldNames.length));
        }

        ObjectNode objectNode = MAPPER.createObjectNode();

        for (int i = 0; i < row.getArity(); i++) {
            JsonNode node = MAPPER.valueToTree(row.getField(i));
            objectNode.set(fieldNames[i], node);
        }

        try {
            return MAPPER.writeValueAsBytes(objectNode);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize row", e);
        }
    }
}
