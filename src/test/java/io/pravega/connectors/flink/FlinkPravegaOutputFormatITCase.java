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

import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.serialization.JsonRowSerializationSchema;
import io.pravega.connectors.flink.utils.SetupUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.apache.flink.table.api.BatchTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

@Slf4j
public class FlinkPravegaOutputFormatITCase extends StreamingMultipleProgramsTestBase {

    /** Setup utility */
    private static final SetupUtils SETUP_UTILS = new SetupUtils();

    @Rule
    public final Timeout globalTimeout = new Timeout(120, TimeUnit.SECONDS);

    // ------------------------------------------------------------------------

    @BeforeClass
    public static void setupPravega() throws Exception {
        SETUP_UTILS.startAllServices();
    }

    @AfterClass
    public static void tearDownPravega() throws Exception {
        SETUP_UTILS.stopAllServices();
    }

    /**
     * Verifies that the output format:
     *  - is able to write properly into Pravega using {@link FlinkPravegaOutputFormat}.
     *  - the data written can be read correctly using {@link FlinkPravegaJsonTableSource}.
     */
    @Test
    public void testPravegaOutputFormat() throws Exception {

        Stream stream = Stream.of(SETUP_UTILS.getScope(), "outputFormatTest");
        SETUP_UTILS.createTestStream(stream.getStreamName(), 1);

        PravegaConfig pravegaConfig = SETUP_UTILS.getPravegaConfig();

        final String[] fields = {"name"};

        SerializationSchema<Row> jsonRowSerializationSchema = new JsonRowSerializationSchema(fields);

        FlinkPravegaOutputFormat<Row> flinkPravegaOutputFormat = FlinkPravegaOutputFormat.<Row>builder()
                                                                .withEventRouter((PravegaEventRouter<Row>) row -> {
                                                                    return (String) row.getField(0);
                                                                })
                                                                .withSerializationSchema(jsonRowSerializationSchema)
                                                                .withPravegaConfig(pravegaConfig)
                                                                .forStream(stream)
                                                                .build();

        flinkPravegaOutputFormat.open(0, 1);

        Row row = new Row(1);

        row.setField(0, "foo");

        flinkPravegaOutputFormat.writeRecord(row);

        flinkPravegaOutputFormat.close();

        // Read the data using Table API
        ExecutionEnvironment execEnvRead = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(execEnvRead);
        execEnvRead.setParallelism(1);

        TableSchema tableSchema = TableSchema.builder().field("name", Types.STRING()).build();

        FlinkPravegaJsonTableSource source = FlinkPravegaJsonTableSource.builder()
                .forStream(stream)
                .withPravegaConfig(pravegaConfig)
                .failOnMissingField(true)
                .withSchema(tableSchema)
                .withReaderGroupScope(stream.getScope())
                .build();

        tableEnv.registerTableSource("OutputFormatTable", source);

        String sqlQuery = "select name from OutputFormatTable";
        Table result = tableEnv.sqlQuery(sqlQuery);

        DataSet<Row> resultSet = ((org.apache.flink.table.api.java.BatchTableEnvironment) tableEnv).toDataSet(result, Row.class);
        List<Row> results = resultSet.collect();

        assertTrue("no records found", results.size() == 1);
    }

}
