/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.connectors.flink;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import io.pravega.client.batch.SegmentRange;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class PravegaInputSplitTest {

    @Mock
    SegmentRange  range1;

    @Mock
    SegmentRange range2;

    @Mock
    SegmentRange range3;

    @Mock
    PravegaInputSplit mockSplitNullSegmentRange;

    @Mock
    PravegaInputSplit mockSplitNullSegmentRange2;


    @Before
    public void setupMocks() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testEquals() {
        PravegaInputSplit split1 = new PravegaInputSplit(12345, range1);
        PravegaInputSplit split2 = new PravegaInputSplit(12345, range1);

        // compare one to itself
        assertTrue(split1.equals(split1));

        // compare one to the other
        assertTrue(split1.equals(split2));

    }

    @Test
    public void testNotEquals() {
        //create 2 different input splits with different ranges

        PravegaInputSplit split1 = new PravegaInputSplit(12345, range1);
        PravegaInputSplit split2 = new PravegaInputSplit(12345, range2);
        assertFalse(split1.equals(split2));

        PravegaInputSplit split3 = new PravegaInputSplit(12345, range1);
        PravegaInputSplit split4 = new PravegaInputSplit(67890, range1);
        assertFalse(split3.equals(split4));

        String str = "dummy";
        assertFalse(split3.equals(str));

    }

}