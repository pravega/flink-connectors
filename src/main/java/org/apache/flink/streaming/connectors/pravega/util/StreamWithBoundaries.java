/**
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

package org.apache.flink.streaming.connectors.pravega.util;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import lombok.Data;
import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * A Pravega stream with optional boundaries based on stream cuts.
 */
@Data
@Internal
public class StreamWithBoundaries implements Serializable {

    private static final long serialVersionUID = 1L;

    @SuppressFBWarnings("SE_BAD_FIELD")
    private final Stream stream;
    private final StreamCut from;
    private final StreamCut to;

    public static StreamWithBoundaries of(Stream stream, StreamCut from, StreamCut to) {
        Preconditions.checkNotNull(stream, "stream");
        Preconditions.checkNotNull(from, "from");
        Preconditions.checkNotNull(to, "to");
        return new StreamWithBoundaries(stream, from, to);
    }
}
