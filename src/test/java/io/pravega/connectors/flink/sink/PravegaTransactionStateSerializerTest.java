package io.pravega.connectors.flink.sink;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class PravegaTransactionStateSerializerTest {

    private static final PravegaTransactionStateSerializer SERIALIZER = new PravegaTransactionStateSerializer();

    @Test
    public void testTransactionStateSerDe() throws IOException {
        final String transactionId = "00000000-0000-0000-0000-000000000001";
        final long watermark = 200;
        final PravegaTransactionState transactionState = new PravegaTransactionState(transactionId, watermark);
        final byte[] serialized = SERIALIZER.serialize(transactionState);
        assertEquals(transactionState, SERIALIZER.deserialize(1, serialized));
    }
}
