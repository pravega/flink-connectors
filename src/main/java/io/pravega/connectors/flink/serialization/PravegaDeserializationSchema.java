/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.connectors.flink.serialization;

import io.pravega.client.stream.Serializer;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.AbstractDeserializationSchema;

/**
 * A deserialization schema adapter for a Pravega serializer.
 */
public class PravegaDeserializationSchema<T extends Serializable> extends AbstractDeserializationSchema<T> {
    private final Class<T> typeClass;
    private final Serializer<T> serializer;

    public PravegaDeserializationSchema(Class<T> typeClass, Serializer<T> serializer) {
        this.typeClass = typeClass;
        this.serializer = serializer;
    }

    @Override
    public T deserialize(byte[] message) throws IOException {
        ByteBuffer msg = ByteBuffer.wrap(message);
        return serializer.deserialize(msg);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(typeClass);
    }

    public Serializer<T> getSerializer() {
        return serializer;
    }
}
