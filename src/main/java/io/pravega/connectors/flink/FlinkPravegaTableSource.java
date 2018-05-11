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

import io.pravega.client.ClientConfig;
import io.pravega.connectors.flink.util.StreamWithBoundaries;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link TableSource} to read Pravega streams using the Flink Table API.
 *
 * Supports both stream and batch environments.
 */
public abstract class FlinkPravegaTableSource implements StreamTableSource<Row>, BatchTableSource<Row> {

    private final Supplier<FlinkPravegaReader<Row>> sourceFunctionFactory;

    private final Supplier<FlinkPravegaInputFormat<Row>> inputFormatFactory;

    private final TableSchema schema;

    /** Type information describing the result type. */
    private final TypeInformation<Row> returnType;

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
        return env.createInput(inputFormat);
    }

    @Override
    public TypeInformation<Row> getReturnType() {
        return returnType;
    }

    @Override
    public TableSchema getTableSchema() {
        return schema;
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
