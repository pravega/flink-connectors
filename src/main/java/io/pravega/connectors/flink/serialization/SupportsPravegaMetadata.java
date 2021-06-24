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
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface SupportsPravegaMetadata<T> {
    void deserialize(byte[] message, EventRead<ByteBuffer> eventRead, Collector<T> out) throws IOException;

}
