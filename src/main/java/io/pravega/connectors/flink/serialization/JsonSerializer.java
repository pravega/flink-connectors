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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.pravega.client.stream.Serializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

public class JsonSerializer<T> implements Serializer<T>, Serializable {

    /** Object mapper for parsing the JSON. */
    private final ObjectMapper objectMapper = new ObjectMapper();
    private Class<T> valueType;

    public JsonSerializer(Class<T> valueType) {
        this.valueType = valueType;
    }

    @Override
    public ByteBuffer serialize(T value) {
        byte[] bytes = new byte[0];
        try {
            bytes = objectMapper.writeValueAsBytes(value);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ByteBuffer.wrap(bytes);
    }

    @Override
    public T deserialize(ByteBuffer serializedValue) {
        ByteArrayInputStream bin = new ByteArrayInputStream(serializedValue.array(),
                serializedValue.position(),
                serializedValue.remaining());
        T event = null;
        try {
            event = objectMapper.readValue(bin, valueType);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return event;
    }
}
