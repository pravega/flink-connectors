package io.pravega.connectors.flink.sink.comitter;

import io.pravega.client.stream.Transaction;
import org.apache.flink.api.connector.sink.Committer;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class SinkCommitter<T> implements Committer<Transaction<T>> {
    @Override
    public List<Transaction<T>> commit(List<Transaction<T>> committables) throws IOException {
        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {
        // Do nothing.
    }
}
