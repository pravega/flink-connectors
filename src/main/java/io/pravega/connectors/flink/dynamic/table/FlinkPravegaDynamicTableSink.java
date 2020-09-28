package io.pravega.connectors.flink.dynamic.table;

import io.pravega.connectors.flink.FlinkPravegaOutputFormat;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaEventRouter;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkArgument;

public class FlinkPravegaDynamicTableSink implements DynamicTableSink {

    /** The schema of the table. */
    protected TableSchema schema;

    /** A factory for the stream writer. */
    protected final Function<TableSchema, FlinkPravegaWriter<RowData>> writerFactory;

    /** A factory for output format. */
    protected final Function<TableSchema, FlinkPravegaOutputFormat<RowData>> outputFormatFactory;

    /** Sink format for encoding records to Pravega. */
    protected final EncodingFormat<SerializationSchema<RowData>> encodingFormat;

    /**
     * Creates a Pravega {@link AppendStreamTableSink}.
     *
     * <p>Each row is written to a Pravega stream with a routing key based on the {@code routingKeyFieldName}.
     * The specified field must of type {@code STRING}.
     *
     * @param schema                       The table schema of the sink.
     * @param writerFactory                A factory for the stream writer.
     * @param outputFormatFactory          A factory for the output format.
     * @param encodingFormat               The sink format for encoding records to Pravega.
     */
    protected FlinkPravegaDynamicTableSink(TableSchema schema,
                                           Function<TableSchema, FlinkPravegaWriter<RowData>> writerFactory,
                                           Function<TableSchema, FlinkPravegaOutputFormat<RowData>> outputFormatFactory,
                                           EncodingFormat<SerializationSchema<RowData>> encodingFormat) {
        this.schema = Preconditions.checkNotNull(schema, "Schema must not be null.");
        this.writerFactory = Preconditions.checkNotNull(writerFactory, "WriterFactory must not be null.");
        this.outputFormatFactory = Preconditions.checkNotNull(outputFormatFactory, "OutputFormatFactory must not be null.");
        this.encodingFormat = Preconditions.checkNotNull(encodingFormat, "Encoding format must not be null.");
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return this.encodingFormat.getChangelogMode();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        FlinkPravegaWriter<RowData> writer = writerFactory.apply(schema);

        return SinkFunctionProvider.of(writer);
    }

    @Override
    public DynamicTableSink copy() {
        return new FlinkPravegaDynamicTableSink(
                this.schema,
                this.writerFactory,
                this.outputFormatFactory,
                this.encodingFormat);
    }

    @Override
    public String asSummaryString() {
        return "Flink Pravega Dynamic Table Sink";
    }

    /**
     * An event router that extracts the routing key from a {@link RowData} by field name.
     */
    public static class RowBasedRouter implements PravegaEventRouter<RowData> {

        private final int keyIndex;

        public RowBasedRouter(String keyFieldName, String[] fieldNames, DataType[] fieldTypes) {
            checkArgument(fieldNames.length == fieldTypes.length,
                    "Number of provided field names and types does not match.");
            int keyIndex = Arrays.asList(fieldNames).indexOf(keyFieldName);
            checkArgument(keyIndex >= 0,
                    "Key field '" + keyFieldName + "' not found");
            checkArgument(DataTypes.STRING().equals(fieldTypes[keyIndex]),
                    "Key field must be of type 'STRING'");
            this.keyIndex = keyIndex;
        }

        @Override
        public String getRoutingKey(RowData event) {
            return event.getString(keyIndex).toString();
        }

        int getKeyIndex() {
            return keyIndex;
        }
    }
}
