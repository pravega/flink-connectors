/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.flink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link TableSource} to read Pravega streams using the Flink Table API.
 *
 * Supports both stream and batch environments.
 * @deprecated Please use the new Table API {@link io.pravega.connectors.flink.dynamic.table.FlinkPravegaDynamicTableSource}
 */
@Deprecated
public class FlinkPravegaTableSource implements StreamTableSource<Row>, BatchTableSource<Row>,
        DefinedProctimeAttribute, DefinedRowtimeAttributes {

    private final Supplier<FlinkPravegaReader<Row>> sourceFunctionFactory;

    private final Supplier<FlinkPravegaInputFormat<Row>> inputFormatFactory;

    private final TableSchema schema;

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
    protected FlinkPravegaTableSource(
            Supplier<FlinkPravegaReader<Row>> sourceFunctionFactory,
            Supplier<FlinkPravegaInputFormat<Row>> inputFormatFactory,
            TableSchema schema) {
        this.sourceFunctionFactory = checkNotNull(sourceFunctionFactory, "sourceFunctionFactory");
        this.inputFormatFactory = checkNotNull(inputFormatFactory, "inputFormatFactory");
        this.schema = TableSchemaUtils.checkOnlyPhysicalColumns(schema);
    }

    /**
     * NOTE: This method is for internal use only for defining a TableSource.
     *       Do not use it in Table API programs.
     */
    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment env) {
        FlinkPravegaReader<Row> reader = sourceFunctionFactory.get();
        reader.initialize();
        return env.addSource(reader).name(explainSource());
    }

    /**
     * NOTE: This method is for internal use only for defining a TableSource.
     *       Do not use it in Table API programs.
     */
    @Override
    public DataSet<Row> getDataSet(ExecutionEnvironment env) {
        FlinkPravegaInputFormat<Row> inputFormat = inputFormatFactory.get();
        return env.createInput(inputFormat, getProducedTypeInformation()).name(explainSource());
    }

    @Override
    public DataType getProducedDataType() {
        return schema.toRowDataType();
    }

    @SuppressWarnings("unchecked")
    private TypeInformation<Row> getProducedTypeInformation() {
        return (TypeInformation<Row>) TypeConversions.fromDataTypeToLegacyInfo(getProducedDataType());
    }

    @Override
    public TableSchema getTableSchema() {
        return schema;
    }

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

    @Override
    public String explainSource() {
        return TableConnectorUtils.generateRuntimeName(this.getClass(), schema.getFieldNames());
    }
}
