/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 */
package io.pravega.connectors.flink;

import io.pravega.client.stream.Checkpoint;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

/**
 * Simple serializer for {@link Checkpoint} objects.
 *
 * <p>The serializer currently uses {@link java.io.Serializable Java Serialization} to
 * serialize the checkpoint objects.
 */
class CheckpointSerializer implements SimpleVersionedSerializer<Checkpoint> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(Checkpoint checkpoint) throws IOException {
        return SerializationUtils.serialize(checkpoint);
    }

    @Override
    public Checkpoint deserialize(int version, byte[] bytes) throws IOException {
        if (version != VERSION) {
            throw new IOException("Invalid format version for serialized Pravega Checkpoint: " + version);
        }

        return (Checkpoint) SerializationUtils.deserialize(bytes);
    }
}
