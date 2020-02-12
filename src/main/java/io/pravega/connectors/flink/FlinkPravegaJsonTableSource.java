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

import io.pravega.connectors.flink.watermark.AssignerWithTimeWindows;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;
import org.apache.flink.util.SerializedValue;

import java.util.function.Supplier;

/**
 * A {@link TableSource} to read JSON-formatted Pravega streams using the Flink Table API.
 *
 * @deprecated Use the {@link Pravega} descriptor along with schema and format descriptors to define {@link FlinkPravegaTableSource}
 * See {@link org.apache.flink.table.api.TableEnvironment#connect(ConnectorDescriptor)}for more details on descriptors.
 */
@Deprecated
public class FlinkPravegaJsonTableSource extends FlinkPravegaTableSource {

    protected FlinkPravegaJsonTableSource(
            Supplier<FlinkPravegaReader<Row>> readerFactory,
            Supplier<FlinkPravegaInputFormat<Row>> inputFormatFactory,
            TableSchema tableSchema) {
        super(readerFactory, inputFormatFactory, tableSchema, jsonSchemaToReturnType(tableSchema));
    }

    @Override
    public String explainSource() {
        return "FlinkPravegaJsonTableSource";
    }

    /**
     * A builder for {@link FlinkPravegaJsonTableSource} to read Pravega streams using the Flink Table API.
     */
    public static FlinkPravegaJsonTableSource.Builder builder() {
        return new Builder();
    }

    /** Converts the JSON schema into into the return type. */
    private static RowTypeInfo jsonSchemaToReturnType(TableSchema jsonSchema) {
        return new RowTypeInfo(jsonSchema.getFieldTypes(), jsonSchema.getFieldNames());
    }

    /**
     * A builder for {@link FlinkPravegaJsonTableSource} to read JSON-formatted Pravega streams using the Flink Table API.
     */
    public static class Builder
            extends FlinkPravegaTableSource.BuilderBase<FlinkPravegaJsonTableSource, Builder> {

        private boolean failOnMissingField = false;

        @Override
        protected Builder builder() {
            return this;
        }

        /**
         * Sets flag whether to fail if a field is missing.
         *
         * @param failOnMissingField If set to true, the TableSource fails if a missing fields.
         *                           If set to false, a missing field is set to null.
         * @return The builder.
         */
        public Builder failOnMissingField(boolean failOnMissingField) {
            this.failOnMissingField = failOnMissingField;
            return builder();
        }

        @Override
        @SuppressWarnings("deprecation")
        protected io.pravega.connectors.flink.serialization.JsonRowDeserializationSchema getDeserializationSchema() {
            io.pravega.connectors.flink.serialization.JsonRowDeserializationSchema deserSchema = new
                    io.pravega.connectors.flink.serialization.JsonRowDeserializationSchema(jsonSchemaToReturnType(getTableSchema()));
            deserSchema.setFailOnMissingField(failOnMissingField);
            return deserSchema;
        }

        @Override
        protected SerializedValue<AssignerWithTimeWindows<Row>> getAssignerWithTimeWindows() {
            return null;
        }

        /**
         * Builds a {@link FlinkPravegaReader} based on the configuration.
         *
         * @throws IllegalStateException if the configuration is invalid.
         */
        public FlinkPravegaJsonTableSource build() {
            FlinkPravegaJsonTableSource tableSource = new FlinkPravegaJsonTableSource(
                    this::buildSourceFunction,
                    this::buildInputFormat,
                    getTableSchema());
            configureTableSource(tableSource);
            return tableSource;
        }
    }
}
