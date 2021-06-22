package io.pravega.connectors.flink;


import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.util.Collector;

import java.util.ArrayDeque;
import java.util.Queue;

public class PravegaCollector<T> implements Collector<T> {
    private final DeserializationSchema<T> deserializationSchema;

    private final Queue<T> records = new ArrayDeque<>();

    private boolean endOfStreamSignalled = false;

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
    public void close() {}
}
