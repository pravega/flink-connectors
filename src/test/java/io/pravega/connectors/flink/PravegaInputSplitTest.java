/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.connectors.flink;

import io.pravega.client.batch.SegmentRange;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.assertj.core.api.Assertions.assertThat;

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


    @BeforeEach
    public void setupMocks() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testEquals() {
        PravegaInputSplit split1 = new PravegaInputSplit(12345, range1);
        PravegaInputSplit split2 = new PravegaInputSplit(12345, range1);

        // compare one to itself
        assertThat(split1.equals(split1)).isTrue();

        // compare one to the other
        assertThat(split1.equals(split2)).isTrue();

    }

    @Test
    public void testNotEquals() {
        //create 2 different input splits with different ranges

        PravegaInputSplit split1 = new PravegaInputSplit(12345, range1);
        PravegaInputSplit split2 = new PravegaInputSplit(12345, range2);
        assertThat(split1.equals(split2)).isFalse();

        PravegaInputSplit split3 = new PravegaInputSplit(12345, range1);
        PravegaInputSplit split4 = new PravegaInputSplit(67890, range1);
        assertThat(split3.equals(split4)).isFalse();

        String str = "dummy";
        assertThat(split3).isNotEqualTo(str);

    }

}