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

package org.apache.flink.streaming.connectors.pravega.utils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

/**
 * A test harness for {@link StreamSink StreamSink} operators and {@link SinkFunction SinkFunctions}.
 *
 * @param <T> The input type of the sink.
 */
public class StreamSinkOperatorTestHarness<T> extends OneInputStreamOperatorTestHarness<T, Object> implements AutoCloseable {

    public StreamSinkOperatorTestHarness(SinkFunction<T> function, TypeSerializer<T> typeSerializerIn) throws Exception {
        this(new StreamSink<>(function), typeSerializerIn);
    }

    public StreamSinkOperatorTestHarness(StreamSink<T> operator, TypeSerializer<T> typeSerializerIn) throws Exception {
        super(operator, typeSerializerIn);
    }
}
