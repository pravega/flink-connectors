package io.pravega.connectors.flink.dynamic.table;

import io.pravega.connectors.flink.FlinkPravegaInputFormat;
import io.pravega.connectors.flink.FlinkPravegaReader;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.sources.*;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class FlinkPravegaDynamicTableSource implements ScanTableSource, DefinedProctimeAttribute, DefinedRowtimeAttributes{

    private final Supplier<FlinkPravegaReader<RowData>> sourceFunctionFactory;

    private final Supplier<FlinkPravegaInputFormat<RowData>> inputFormatFactory;

    private final TableSchema schema;

    /** Scan format for decoding records from Pravega. */
    protected final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

    /** Field name of the processing time attribute, null if no processing time field is defined. */
    private String proctimeAttribute;

    /** Descriptor for a rowtime attribute. */
    private List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors;

    /**
     * Creates a Pravega {@link TableSource}.
     * @param sourceFunctionFactory a factory for the {@link FlinkPravegaReader} to implement {@link StreamTableSource}
     * @param inputFormatFactory a factory for the {@link FlinkPravegaInputFormat} to implement {@link BatchTableSource}
     * @param schema the table schema
     */
    protected FlinkPravegaDynamicTableSource(
            Supplier<FlinkPravegaReader<RowData>> sourceFunctionFactory,
            Supplier<FlinkPravegaInputFormat<RowData>> inputFormatFactory,
            TableSchema schema,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat) {
        this.sourceFunctionFactory = checkNotNull(sourceFunctionFactory, "sourceFunctionFactory");
        this.inputFormatFactory = checkNotNull(inputFormatFactory, "inputFormatFactory");
        this.schema = TableSchemaUtils.checkNoGeneratedColumns(schema);
        this.decodingFormat = Preconditions.checkNotNull(
                decodingFormat, "Decoding format must not be null.");
    }


    @Override
    public ChangelogMode getChangelogMode() {
        return this.decodingFormat.getChangelogMode();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        FlinkPravegaReader<RowData> reader = sourceFunctionFactory.get();
        //reader.initialize();
        return SourceFunctionProvider.of(reader, false);
    }

    @Override
    public DynamicTableSource copy() {
        return new FlinkPravegaDynamicTableSource(
                this.sourceFunctionFactory,
                this.inputFormatFactory,
                this.schema,
                this.decodingFormat);
    }

    @Override
    public String asSummaryString() {
        return "Flink Pravega Dynamic Table Source";
    }

    @Nullable
    @Override
    public String getProctimeAttribute() {
        return proctimeAttribute;
    }

    @Override
    public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
        return rowtimeAttributeDescriptors;
    }

    /**
     * Declares a field of the schema to be the processing time attribute.
     *
     * @param proctimeAttribute The name of the field that becomes the processing time field.
     */
    protected void setProctimeAttribute(String proctimeAttribute) {
        if (proctimeAttribute != null) {
            // validate that field exists and is of correct type
            Optional<DataType> tpe = schema.getFieldDataType(proctimeAttribute);
            if (!tpe.isPresent()) {
                throw new ValidationException("Processing time attribute " + proctimeAttribute + " is not present in TableSchema.");
            } else if (tpe.get().getLogicalType().getTypeRoot() != LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) {
                throw new ValidationException("Processing time attribute " + proctimeAttribute + " is not of type TIMESTAMP.");
            }
        }
        this.proctimeAttribute = proctimeAttribute;
    }

    /**
     * Declares a list of fields to be rowtime attributes.
     *
     * @param rowtimeAttributeDescriptors The descriptors of the rowtime attributes.
     */
    protected void setRowtimeAttributeDescriptors(List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors) {
        // validate that all declared fields exist and are of correct type
        for (RowtimeAttributeDescriptor desc : rowtimeAttributeDescriptors) {
            String rowtimeAttribute = desc.getAttributeName();
            Optional<DataType> tpe = schema.getFieldDataType(rowtimeAttribute);
            if (!tpe.isPresent()) {
                throw new ValidationException("Rowtime attribute " + rowtimeAttribute + " is not present in TableSchema.");
            } else if (tpe.get().getLogicalType().getTypeRoot() != LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) {
                throw new ValidationException("Rowtime attribute " + rowtimeAttribute + " is not of type TIMESTAMP.");
            }
        }
        this.rowtimeAttributeDescriptors = rowtimeAttributeDescriptors;
    }
}
