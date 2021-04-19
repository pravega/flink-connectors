package io.pravega.connectors.flink.sink.writer;

import io.pravega.client.stream.Transaction;
import org.apache.flink.api.connector.sink.SinkWriter;

import java.io.IOException;
import java.util.List;

public class PravegaWriter<T> implements SinkWriter<T, Transaction<T>, Void> {
    @Override
    public void write(T element, Context context) throws IOException {

    }

    @Override
    public List<Transaction<T>> prepareCommit(boolean flush) throws IOException {
        return null;
    }

    @Override
    public List<Void> snapshotState() throws IOException {
        return null;
    }

    @Override
    public void close() throws Exception {

    }
}
