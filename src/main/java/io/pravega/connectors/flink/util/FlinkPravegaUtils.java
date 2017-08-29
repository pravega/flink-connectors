package io.pravega.connectors.flink.util;

import io.pravega.connectors.flink.EventTimeOrderingOperator;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;

public class FlinkPravegaUtils {

    /**
     * Writes a stream of elements to a Pravega stream with event time ordering.
     * <p>
     * This method returns a sink that automatically reorders events by their event timestamp.  The ordering
     * is preserved by Pravega using the associated routing key.
     *
     * @param stream      the stream to read.
     * @param writer      the Pravega writer to use.
     * @param parallelism the degree of parallelism for the writer.
     * @param <T>         The type of the event.
     * @return a sink.
     */
    public static <T> DataStreamSink<T> writeToPravegaInEventTimeOrder(DataStream<T> stream, FlinkPravegaWriter<T> writer, int parallelism) {
        // a keyed stream is used to ensure that all elements for a given key are forwarded to the same writer instance.
        // the parallelism must match between the ordering operator and the sink operator to ensure that
        // a forwarding strategy (as opposed to a rebalancing strategy) is used by Flink between the two operators.
        return stream
                .keyBy(new PravegaEventRouterKeySelector<>(writer.getEventRouter()))
                .transform("reorder", stream.getType(), new EventTimeOrderingOperator<>()).setParallelism(parallelism)
                .addSink(writer).setParallelism(parallelism);
    }

    private FlinkPravegaUtils() {
    }
}
