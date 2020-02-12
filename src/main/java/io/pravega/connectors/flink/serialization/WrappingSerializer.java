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

/**
 * Interface for serializers that wrap Pravega's {@link Serializer}.
 */
public interface WrappingSerializer<T>  {

    /**
     * Gets the wrapped Pravega Serializer.
     */
    Serializer<T> getWrappedSerializer();
}
