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

import io.pravega.client.ClientConfig;
import io.pravega.connectors.flink.util.StreamWithBoundaries;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.tsextractors.TimestampExtractor;
import org.apache.flink.table.sources.wmstrategies.WatermarkStrategy;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
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

    /**
     * A base builder for {@link FlinkPravegaTableSource} to read Pravega streams using the Flink Table API.
     *
     * @param <T> the table source type.
     * @param <B> the builder type.
     */
    public abstract static class BuilderBase<T extends FlinkPravegaTableSource, B extends AbstractStreamingReaderBuilder>
            extends AbstractStreamingReaderBuilder<Row, B> {

        private TableSchema schema;

        private String proctimeAttribute;

        private RowtimeAttributeDescriptor rowtimeAttributeDescriptor;

        /**
         * Sets the schema of the produced table.
         *
         * @param schema The schema of the produced table.
         * @return The builder.
         */
        public B withSchema(TableSchema schema) {
            Preconditions.checkNotNull(schema, "Schema must not be null.");
            Preconditions.checkArgument(this.schema == null, "Schema has already been set.");
            this.schema = schema;
            return builder();
        }

        /**
         * Configures a field of the table to be a processing time attribute.
         * The configured field must be present in the table schema and of type {@link Types#SQL_TIMESTAMP}.
         *
         * @param proctimeAttribute The name of the processing time attribute in the table schema.
         * @return The builder.
         */
        public B withProctimeAttribute(String proctimeAttribute) {
            Preconditions.checkNotNull(proctimeAttribute, "Proctime attribute must not be null.");
            Preconditions.checkArgument(!proctimeAttribute.isEmpty(), "Proctime attribute must not be empty.");
            Preconditions.checkArgument(this.proctimeAttribute == null, "Proctime attribute has already been set.");
            this.proctimeAttribute = proctimeAttribute;
            return builder();
        }

        /**
         * Configures a field of the table to be a rowtime attribute.
         * The configured field must be present in the table schema and of type {@link Types#SQL_TIMESTAMP}.
         *
         * @param rowtimeAttribute The name of the rowtime attribute in the table schema.
         * @param timestampExtractor The {@link TimestampExtractor} to extract the rowtime attribute from the physical type.
         * @param watermarkStrategy The {@link WatermarkStrategy} to generate watermarks for the rowtime attribute.
         * @return The builder.
         */
        public B withRowtimeAttribute(
                String rowtimeAttribute,
                TimestampExtractor timestampExtractor,
                WatermarkStrategy watermarkStrategy) {
            Preconditions.checkNotNull(rowtimeAttribute, "Rowtime attribute must not be null.");
            Preconditions.checkArgument(!rowtimeAttribute.isEmpty(), "Rowtime attribute must not be empty.");
            Preconditions.checkNotNull(timestampExtractor, "Timestamp extractor must not be null.");
            Preconditions.checkNotNull(watermarkStrategy, "Watermark assigner must not be null.");
            Preconditions.checkArgument(this.rowtimeAttributeDescriptor == null,
                    "Currently, only one rowtime attribute is supported.");

            this.rowtimeAttributeDescriptor = new RowtimeAttributeDescriptor(
                    rowtimeAttribute,
                    timestampExtractor,
                    watermarkStrategy);
            return builder();
        }

        /**
         * Returns the configured table schema.
         *
         * @return the configured table schema.
         */
        protected TableSchema getTableSchema() {
            Preconditions.checkState(this.schema != null, "Schema hasn't been set.");
            return this.schema;
        }

        /**
         * Applies a configuration to the table source.
         *
         * @param source the table source.
         */
        protected void configureTableSource(T source) {
            // configure processing time attributes
            source.setProctimeAttribute(proctimeAttribute);
            // configure rowtime attributes
            if (rowtimeAttributeDescriptor == null) {
                source.setRowtimeAttributeDescriptors(Collections.emptyList());
            } else {
                source.setRowtimeAttributeDescriptors(Collections.singletonList(rowtimeAttributeDescriptor));
            }
        }

        /**
         * Gets a factory to build an {@link FlinkPravegaInputFormat} for using the Table API in a Flink batch environment.
         *
         * @return a supplier to eagerly validate the configuration and lazily construct the input format.
         */
        FlinkPravegaInputFormat<Row> buildInputFormat() {

            final List<StreamWithBoundaries> streams = resolveStreams();
            final ClientConfig clientConfig = getPravegaConfig().getClientConfig();
            final DeserializationSchema<Row> deserializationSchema = getDeserializationSchema();

            return new FlinkPravegaInputFormat<>(clientConfig, streams, deserializationSchema);
        }
    }
}
