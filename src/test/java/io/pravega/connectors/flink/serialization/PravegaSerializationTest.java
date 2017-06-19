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

import java.io.IOException;
import org.junit.Test;
import static org.junit.Assert.*;

public class PravegaSerializationTest {
    @Test
    public void testSerialization() throws IOException {
        PravegaSerializationSchema<String> serializer = PravegaSerialization.serializationFor(String.class);
        PravegaDeserializationSchema<String> deserializer = PravegaSerialization.deserializationFor(String.class);

        String input = "Testing input";
        byte[] serialized = serializer.serialize(input);
        assertEquals(input, deserializer.deserialize(serialized));
    }


}
