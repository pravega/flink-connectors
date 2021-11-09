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
package io.pravega.connectors.flink.sink;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class PravegaTransactionStateSerializerTest {

    private static final PravegaTransactionStateSerializer SERIALIZER = new PravegaTransactionStateSerializer();

    @Test
    public void testTransactionStateSerDe() throws IOException {
        final String transactionId = "00000000-0000-0000-0000-000000000001";
        final PravegaTransactionState transactionState = new PravegaTransactionState(transactionId);
        final byte[] serialized = SERIALIZER.serialize(transactionState);
        assertEquals(transactionState, SERIALIZER.deserialize(1, serialized));
    }
}
