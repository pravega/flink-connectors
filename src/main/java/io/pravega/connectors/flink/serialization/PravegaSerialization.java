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

import io.pravega.client.stream.impl.JavaSerializer;
import java.io.Serializable;

/**
 * Helper methods to create DeserializationSchema and SerializationSchemas using the pravega JavaSerializer
 * for generic types.
 *
 * @deprecated Please use the constructor with {@link JavaSerializer} instead.
 */
@Deprecated
public class PravegaSerialization {
    public static final <T extends Serializable> PravegaDeserializationSchema<T> deserializationFor(Class<T> type) {
        return new PravegaDeserializationSchema<>(type, new JavaSerializer<>());
    }

    public static final <T extends Serializable> PravegaSerializationSchema<T> serializationFor(Class<T> type) {
        return new PravegaSerializationSchema<>(new JavaSerializer<T>());
    }
}
