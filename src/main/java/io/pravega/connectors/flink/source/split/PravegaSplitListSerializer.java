/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.flink.source.split;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.util.List;

public class PravegaSplitListSerializer implements SimpleVersionedSerializer<List<PravegaSplit>> {
    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(List<PravegaSplit> obj) throws IOException {
        return InstantiationUtil.serializeObject(obj);
    }

    @Override
    public List<PravegaSplit> deserialize(int version, byte[] serialized) throws IOException {
        try {
            return InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());
        } catch (ClassNotFoundException e) {
            throw new FlinkRuntimeException("Failed to deserialize the enumerator checkpoint.");
        }
    }
}