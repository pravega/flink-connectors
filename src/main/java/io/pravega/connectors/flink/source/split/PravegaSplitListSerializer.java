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

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.util.List;

public class PravegaSplitListSerializer implements SimpleVersionedSerializer<List<PravegaSplit>> {
    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(List<PravegaSplit> obj) throws IOException {
        return InstantiationUtil.serializeObject(obj);
    }

    @Override
    public List<PravegaSplit> deserialize(int version, byte[] serialized) throws IOException {
        try {
            return InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());
        } catch (ClassNotFoundException e) {
            throw new FlinkRuntimeException("Failed to deserialize the enumerator checkpoint.");
        }
    }
}