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

import io.pravega.client.stream.EventRead;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A Pravega DeserializationSchema that makes altering event possible. <p>
 *
 * To add the metadata to the row, simply overwrite the {@link #deserialize(byte[], EventRead)} method.
 */
public abstract class PravegaDeserializationSchemaWithMetadata<T> implements DeserializationSchema<T> {
    public abstract T deserialize(byte[] message, EventRead<ByteBuffer> eventRead) throws IOException;

    public void deserialize(byte[] message, EventRead<ByteBuffer> eventRead, Collector<T> out) throws IOException {
        T deserialize = deserialize(message, eventRead);
        if (deserialize != null) {
            out.collect(deserialize);
        }
    }

    public T deserialize(byte[] message) throws IOException {
        throw new IllegalStateException("A collector is required for deserializing.");
    }
}
