package io.pravega.connectors.flink.sink;

import io.pravega.client.stream.Transaction;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class FlinkPravegaSink<T> implements Sink<T, Transaction<T>, Void, Void> {

    @Override
    public SinkWriter<T, Transaction<T>, Void> createWriter(InitContext context, List<Void> states) throws IOException {
        return null;
    }

    @Override
    public Optional<Committer<Transaction<T>>> createCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalCommitter<Transaction<T>, Void>> createGlobalCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Transaction<T>>> getCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getWriterStateSerializer() {
        return Optional.empty();
    }
}
