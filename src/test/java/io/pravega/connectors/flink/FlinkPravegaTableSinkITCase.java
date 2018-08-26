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
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

@Slf4j
public class FlinkPravegaTableSinkITCase extends StreamingMultipleProgramsTestBase {

    /** Setup utility */
    private static final SetupUtils SETUP_UTILS = new SetupUtils();

    private static final TestRecord[] TEST_DATA = {
            new TestRecord(1001, "Java public for dummies", "Tan Ah Teck", 11.11, 11),
            new TestRecord(1002, "More Java for dummies", "Tan Ah Teck", 22.22, 22),
            new TestRecord(1003, "More Java for more dummies", "Mohammad Ali", 33.33, 33),
            new TestRecord(1004, "A Cup of Java", "Kumar", 44.44, 44),
            new TestRecord(1005, "A Teaspoon of Java", "Kevin Jones", 55.55, 55),
            new TestRecord(1006, "A Teaspoon of Java 1.4", "Kevin Jones", 66.66, 66),
            new TestRecord(1007, "A Teaspoon of Java 1.5", "Kevin Jones", 77.77, 77),
            new TestRecord(1008, "A Teaspoon of Java 1.6", "Kevin Jones", 88.88, 88),
            new TestRecord(1009, "A Teaspoon of Java 1.7", "Kevin Jones", 99.99, 99),
            new TestRecord(1010, "A Teaspoon of Java 1.8", "Kevin Jones", null, 1010)
    };

    private static final String[] FIELDS = {"id", "title", "author", "price", "qty"};

    private static final TypeInformation[] FIELD_TYPES = {
            BasicTypeInfo.INT_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.DOUBLE_TYPE_INFO,
            BasicTypeInfo.INT_TYPE_INFO
    };

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

    @Test
    public void testTableSinkOutputFormat() throws Exception {

        //write the test data to a stream using output format
        Stream stream = Stream.of(SETUP_UTILS.getScope(), "source");
        SETUP_UTILS.createTestStream(stream.getStreamName(), 1);

        PravegaConfig pravegaConfig = SETUP_UTILS.getPravegaConfig();

        SerializationSchema<Row> jsonRowSerializationSchema = new JsonRowSerializationSchema(FIELDS);

        FlinkPravegaOutputFormat<Row> flinkPravegaOutputFormat = FlinkPravegaOutputFormat.<Row>builder()
                .withEventRouter((PravegaEventRouter<Row>) row -> {
                    Integer recordId = (Integer) row.getField(0);
                    return Integer.toString(recordId);
                })
                .withSerializationSchema(jsonRowSerializationSchema)
                .withPravegaConfig(pravegaConfig)
                .forStream(stream)
                .build();

        flinkPravegaOutputFormat.open(0, 1);

        List<Row> expectedResults = new ArrayList<>();
        for (TestRecord record: TEST_DATA) {
            Row row = toRow(record);
            flinkPravegaOutputFormat.writeRecord(row);
            expectedResults.add(row);
        }

        flinkPravegaOutputFormat.close();

        // Read the data using Table Source API and Write to a new stream using Table Sink API
        ExecutionEnvironment execEnvRead = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(execEnvRead);
        execEnvRead.setParallelism(1);

        TableSchema tableSchema = TableSchema.builder()
                .field("id", Types.INT())
                .field("title", Types.STRING())
                .field("author", Types.STRING())
                .field("price", Types.DOUBLE())
                .field("qty", Types.INT())
                .build();
        FlinkPravegaJsonTableSource source = FlinkPravegaJsonTableSource.builder()
                .forStream(stream)
                .withPravegaConfig(pravegaConfig)
                .failOnMissingField(true)
                .withSchema(tableSchema)
                .withReaderGroupScope(stream.getScope())
                .build();

        tableEnv.registerTableSource("source", source);

        String sqlQuery = "select id, title, author, price, qty from source";
        Table result = tableEnv.sqlQuery(sqlQuery);

        Stream sink = Stream.of(stream.getScope(), "sink");
        SETUP_UTILS.createTestStream(sink.getStreamName(), 1);

        FlinkPravegaJsonTableSink flinkPravegaJsonTableSink = FlinkPravegaJsonTableSink.<Row>builder()
                                                                    .withPravegaConfig(pravegaConfig)
                                                                    .withRoutingKeyField("author")
                                                                    .forStream(sink)
                                                                    .build();
        FlinkPravegaTableSink flinkPravegaTableSink = flinkPravegaJsonTableSink.configure(FIELDS, FIELD_TYPES);
        result.writeToSink(flinkPravegaTableSink);
        execEnvRead.execute("rewrite");

        // read the data from the new stream and verify
        execEnvRead = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnvironment = TableEnvironment.getTableEnvironment(execEnvRead);
        execEnvRead.setParallelism(1);

        source = FlinkPravegaJsonTableSource.builder()
                .forStream(sink)
                .withPravegaConfig(pravegaConfig)
                .failOnMissingField(true)
                .withSchema(tableSchema)
                .withReaderGroupScope(sink.getScope())
                .build();

        tableEnvironment.registerTableSource("sink", source);

        sqlQuery = "select * from sink";
        Table result1 = tableEnvironment.sqlQuery(sqlQuery);

        DataSet<Row> resultSet = ((org.apache.flink.table.api.java.BatchTableEnvironment) tableEnvironment).toDataSet(result1, Row.class);
        List<Row> results = resultSet.collect();

        boolean verify = compare(results, expectedResults);
        assertTrue("Output does not match expected result", verify);

    }

    private static boolean compare(List<Row> results, List<Row> expectedResults) {
        if (results.size() != expectedResults.size()) {
            return false;
        }
        for (int i = 0; i < results.size(); i++) {
            Row result = results.get(i);
            Row expected = expectedResults.get(i);
            if (!result.equals(expected)) {
                return false;
            }
        }
        return true;
    }

    private static final class TestRecord {

        protected final Integer id;
        protected final String title;
        protected final String author;
        protected final Double price;
        protected final Integer qty;

        private TestRecord(Integer id, String title, String author, Double price, Integer qty) {
            this.id = id;
            this.title = title;
            this.author = author;
            this.price = price;
            this.qty = qty;
        }

    }

    private static Row toRow(TestRecord entry) {
        Row row = new Row(5);
        row.setField(0, entry.id);
        row.setField(1, entry.title);
        row.setField(2, entry.author);
        row.setField(3, entry.price);
        row.setField(4, entry.qty);
        return row;
    }

}
