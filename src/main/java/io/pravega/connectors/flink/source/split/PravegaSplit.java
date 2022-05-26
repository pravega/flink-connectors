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

package io.pravega.connectors.flink.source.split;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;

/**
 * A {@link SourceSplit} implementation.
 *
 * One PravegaSplit is mapped to one Pravega EventStreamReader, to keep this class serializable, we will initiate the
 * EventStreamReader inside {@link io.pravega.connectors.flink.source.reader.PravegaSplitReader}.
 *
 * PravegaSplit only contains information about the reader ID(the same as split ID)
 * and reader group name of the EventStreamReader.
 */
@Internal
public class PravegaSplit implements SourceSplit, Serializable {

    private static final String PREFIX = "flink-reader";
    private final int subtaskId;
    private final String readerGroupName;

    /**
     * A Pravega Split instance represents an EventStreamReader, but with no operations and keep stateless.
     * To keep it serializable, we will not keep the reader inside.
     *
     * @param readerGroupName   The reader group name.
     * @param subtaskId         The subtaskId of source reader.
     */
    public PravegaSplit(String readerGroupName, int subtaskId) {
        this.readerGroupName = readerGroupName;
        this.subtaskId = subtaskId;
    }

    public int getSubtaskId() {
        return subtaskId;
    }

    public String getReaderGroupName() {
        return readerGroupName;
    }

    @Override
    public String splitId() {
        // the splitId will be the Pravega reader Id.
        return splitId(subtaskId);
    }

    public static String splitId(int subtaskId) {
        return PREFIX + "-" + subtaskId;
    }
}
