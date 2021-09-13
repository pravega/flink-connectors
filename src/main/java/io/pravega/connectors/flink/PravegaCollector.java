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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.Queue;

/**
 * A Pravega collector that supports deserializing bytes to several events.
 */
public class PravegaCollector<T> implements Collector<T>, Serializable {
    private static final long serialVersionUID = 1L;

    private final DeserializationSchema<T> deserializationSchema;

    private boolean endOfStreamSignalled = false;

    // internal buffer
    private final Queue<T> records = new ArrayDeque<>();

    public PravegaCollector(DeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void collect(T record) {
        // do not emit subsequent elements if the end of the stream reached
        if (endOfStreamSignalled || deserializationSchema.isEndOfStream(record)) {
            endOfStreamSignalled = true;
            return;
        }
        records.add(record);
    }

    public Queue<T> getRecords() {
        return records;
    }

    public boolean isEndOfStreamSignalled() {
        return endOfStreamSignalled;
    }

    @Override
    public void close() {
        // do nothing here
    }
}
