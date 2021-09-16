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

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;

public class FlinkPravegaTransactionStateSerializerTest extends SerializerTestBase<FlinkPravegaWriter.PravegaTransactionState> {

    @Override
    protected TypeSerializer<FlinkPravegaWriter.PravegaTransactionState> createSerializer() {
        return new FlinkPravegaWriter.TransactionStateSerializer();
    }

    @Override
    protected Class<FlinkPravegaWriter.PravegaTransactionState> getTypeClass() {
        return FlinkPravegaWriter.PravegaTransactionState.class;
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected FlinkPravegaWriter.PravegaTransactionState[] getTestData() {
        //noinspection unchecked
        return new FlinkPravegaWriter.PravegaTransactionState[] {
                new FlinkPravegaWriter.PravegaTransactionState("txnid", 1L),
                new FlinkPravegaWriter.PravegaTransactionState("txnid", null),
                new FlinkPravegaWriter.PravegaTransactionState(),
        };
    }

    @Override
    public void testInstantiate() {
        // this serializer does not support instantiation
    }
}
