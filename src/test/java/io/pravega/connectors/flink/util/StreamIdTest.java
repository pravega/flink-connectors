/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.connectors.flink.util;

import java.io.IOException;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class StreamIdTest {

    @Test
    public void testToString() throws IOException {
        StreamId stream = new StreamId("testScope", "exampleStream");
        assertEquals("testScope/exampleStream", stream.toString());
    }

    @Test
    public void testFromSpec() {
        String input = "testScope/exampleStream";
        assertEquals(input, StreamId.fromSpec(input).toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNotEnoughArgs() {
        StreamId.fromSpec("a");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTooManyArgs() {
        StreamId.fromSpec("a/b/c");
    }
}
