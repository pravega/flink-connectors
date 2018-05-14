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

import io.pravega.connectors.flink.serialization.JsonRowSerializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.types.Row;

import java.util.function.Function;

/**
 * An append-only table sink to emit a streaming table as a Pravega stream containing JSON-formatted events.
 */
public class FlinkPravegaJsonTableSink extends FlinkPravegaTableSink {
    private FlinkPravegaJsonTableSink(Function<TableSinkConfiguration, FlinkPravegaWriter<Row>> writerFactory) {
        super(writerFactory);
    }

    @Override
    protected FlinkPravegaTableSink createCopy() {
        return new FlinkPravegaJsonTableSink(writerFactory);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder for {@link FlinkPravegaJsonTableSink}.
     */
    public static class Builder extends AbstractTableSinkBuilder<Builder> {

        protected Builder builder() {
            return this;
        }

        @Override
        protected SerializationSchema<Row> getSerializationSchema(String[] fieldNames) {
            return new JsonRowSerializationSchema(fieldNames);
        }

        /**
         * Builds the {@link FlinkPravegaJsonTableSink}.
         */
        public FlinkPravegaJsonTableSink build() {
            return new FlinkPravegaJsonTableSink(this::createSinkFunction);
        }
    }
}
