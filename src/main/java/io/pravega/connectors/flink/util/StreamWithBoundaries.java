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
package io.pravega.connectors.flink.util;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Objects;

/**
 * A Pravega stream with optional boundaries based on stream cuts.
 */
@Internal
public class StreamWithBoundaries implements Serializable {

    private static final long serialVersionUID = 1L;

    @SuppressFBWarnings("SE_BAD_FIELD")
    private final Stream stream;
    private final StreamCut from;
    private final StreamCut to;

    public StreamWithBoundaries(Stream stream, StreamCut from, StreamCut to) {
        this.stream = stream;
        this.from = from;
        this.to = to;
    }

    public static StreamWithBoundaries of(Stream stream, StreamCut from, StreamCut to) {
        Preconditions.checkNotNull(stream, "stream");
        Preconditions.checkNotNull(from, "from");
        Preconditions.checkNotNull(to, "to");
        return new StreamWithBoundaries(stream, from, to);
    }

    public Stream getStream() {
        return stream;
    }

    public StreamCut getFrom() {
        return from;
    }

    public StreamCut getTo() {
        return to;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StreamWithBoundaries that = (StreamWithBoundaries) o;
        return Objects.equals(stream, that.stream) && Objects.equals(from, that.from) && Objects.equals(to, that.to);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stream, from, to);
    }
}
