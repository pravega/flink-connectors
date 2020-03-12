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

import io.pravega.connectors.flink.serialization.JsonRowDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link FlinkPravegaJsonTableSource} and its builder.
 */
public class FlinkPravegaJsonTableSourceTest {

    private static final TableSchema SAMPLE_SCHEMA = TableSchema.builder()
            .field("category", DataTypes.STRING())
            .field("value", DataTypes.INT())
            .build();

    @Test
    public void testReturnType() {
        FlinkPravegaJsonTableSource source = FlinkPravegaJsonTableSource.builder()
                .withReaderGroupScope("scope")
                .forStream("scope/stream")
                .withSchema(SAMPLE_SCHEMA)
                .build();
        TypeInformation<Row> expected = new RowTypeInfo(SAMPLE_SCHEMA.getFieldTypes(), SAMPLE_SCHEMA.getFieldNames());
        assertEquals(expected, source.getReturnType());
    }

    @Test
    public void testGetDeserializationSchema() {
        FlinkPravegaJsonTableSource.Builder builder = new FlinkPravegaJsonTableSource.Builder();
        builder
                .withSchema(SAMPLE_SCHEMA)
                .failOnMissingField(true);
        JsonRowDeserializationSchema deserializer = builder.getDeserializationSchema();
        assertTrue(deserializer.getFailOnMissingField());
    }
}
