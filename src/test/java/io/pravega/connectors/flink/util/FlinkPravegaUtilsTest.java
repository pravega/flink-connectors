/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.connectors.flink.util;

import org.junit.Test;
import static org.junit.Assert.assertTrue;

public class FlinkPravegaUtilsTest {

    @Test
    public void testGetReaderName() {
        String testStr = new String(new char[256]).replace('\0', 'a');
        assertTrue(FlinkPravegaUtils.getReaderName(testStr, 1,1).length() < 256);
    }
}