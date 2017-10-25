/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.connectors.flink.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.pravega.client.stream.Serializer;
import lombok.Getter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * A simple JSON serializer.
 * @param <T> the value type.
 */
public final class JsonSerializer<T> implements Serializer<T>, Serializable {

    private final Class<T> valueType;

    @Getter(lazy = true)
    private transient final ObjectMapper mapper = newMapper();

    public JsonSerializer(Class<T> valueType) {
        this.valueType = Preconditions.checkNotNull(valueType);
    }

    @Override
    public ByteBuffer serialize(T value) {
        try {
            return ByteBuffer.wrap(getMapper().writeValueAsBytes(value));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Serialization failure", e);
        }
    }

    @Override
    public T deserialize(ByteBuffer serializedValue) {
        Preconditions.checkArgument(serializedValue.hasArray(), "limitation: supports only array-backed byte buffers");
        ByteArrayInputStream bin = new ByteArrayInputStream(serializedValue.array(),
                serializedValue.position(),
                serializedValue.remaining());
        try {
            return getMapper().readValue(bin, valueType);
        } catch (IOException e) {
            throw new RuntimeException("Serialization failure", e);
        }
    }

    private static ObjectMapper newMapper() {
        return new ObjectMapper();
    }
}
