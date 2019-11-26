/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.flink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
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
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link TableSource} to read Pravega streams using the Flink Table API.
 *
 * Supports both stream and batch environments.
 */
public abstract class FlinkPravegaTableSource implements StreamTableSource<Row>, BatchTableSource<Row>,
        DefinedProctimeAttribute, DefinedRowtimeAttributes {

    private final Supplier<FlinkPravegaReader<Row>> sourceFunctionFactory;

    private final Supplier<FlinkPravegaInputFormat<Row>> inputFormatFactory;

    private final TableSchema schema;

    /** Type information describing the result type. */
    private final TypeInformation<Row> returnType;

    /** Field name of the processing time attribute, null if no processing time field is defined. */
    private String proctimeAttribute;

    /** Descriptor for a rowtime attribute. */
    private List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors;

    /**
     * Creates a Pravega {@link TableSource}.
     * @param sourceFunctionFactory a factory for the {@link FlinkPravegaReader} to implement {@link StreamTableSource}
     * @param inputFormatFactory a factory for the {@link FlinkPravegaInputFormat} to implement {@link BatchTableSource}
     * @param schema the table schema
     * @param returnType the return type based on the table schema
     */
    protected FlinkPravegaTableSource(
            Supplier<FlinkPravegaReader<Row>> sourceFunctionFactory,
            Supplier<FlinkPravegaInputFormat<Row>> inputFormatFactory,
            TableSchema schema,
            TypeInformation<Row> returnType) {
        this.sourceFunctionFactory = checkNotNull(sourceFunctionFactory, "sourceFunctionFactory");
        this.inputFormatFactory = checkNotNull(inputFormatFactory, "inputFormatFactory");
        this.schema = checkNotNull(schema, "schema");
        this.returnType = checkNotNull(returnType, "returnType");
    }

    /**
     * NOTE: This method is for internal use only for defining a TableSource.
     *       Do not use it in Table API programs.
     */
    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment env) {
        FlinkPravegaReader<Row> reader = sourceFunctionFactory.get();
        reader.initialize();
        return env.addSource(reader);
    }

    /**
     * NOTE: This method is for internal use only for defining a TableSource.
     *       Do not use it in Table API programs.
     */
    @Override
    public DataSet<Row> getDataSet(ExecutionEnvironment env) {
        FlinkPravegaInputFormat<Row> inputFormat = inputFormatFactory.get();
        return env.createInput(inputFormat, returnType);
    }

    @Override
    public TypeInformation<Row> getReturnType() {
        return returnType;
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
            Optional<TypeInformation<?>> tpe = schema.getFieldType(proctimeAttribute);
            if (!tpe.isPresent()) {
                throw new ValidationException("Processing time attribute " + proctimeAttribute + " is not present in TableSchema.");
            } else if (tpe.get() != Types.SQL_TIMESTAMP) {
                throw new ValidationException("Processing time attribute " + proctimeAttribute + " is not of type SQL_TIMESTAMP.");
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
            Optional<TypeInformation<?>> tpe = schema.getFieldType(rowtimeAttribute);
            if (!tpe.isPresent()) {
                throw new ValidationException("Rowtime attribute " + rowtimeAttribute + " is not present in TableSchema.");
            } else if (tpe.get() != Types.SQL_TIMESTAMP) {
                throw new ValidationException("Rowtime attribute " + rowtimeAttribute + " is not of type SQL_TIMESTAMP.");
            }
        }
        this.rowtimeAttributeDescriptors = rowtimeAttributeDescriptors;
    }
}
