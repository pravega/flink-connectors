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

import io.pravega.client.stream.Serializer;
import java.nio.ByteBuffer;
import org.apache.flink.api.common.serialization.SerializationSchema;

/**
 * A serialization schema adapter for a Pravega serializer.
 */
public class PravegaSerializationSchema<T> 
        implements SerializationSchema<T>, WrappingSerializer<T> {

    // the Pravega serializer
    private final Serializer<T> serializer;

    public PravegaSerializationSchema(Serializer<T> serializer) {
        this.serializer = serializer;
    }

    @Override
    public byte[] serialize(T element) {
        ByteBuffer buf = serializer.serialize(element);
        
        if (buf.hasArray() && buf.arrayOffset() == 0 && buf.position() == 0 && buf.limit() == buf.capacity()) {
            return buf.array();
        } else {
            byte[] bytes = new byte[buf.remaining()];
            buf.get(bytes);
            return bytes;
        }
    }

    @Override
    public Serializer<T> getWrappedSerializer() {
        return serializer;
    }
}
