/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.flink.dynamic.table;

import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.EventPointer;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaWriterMode;
import io.pravega.connectors.flink.serialization.JsonSerializer;
import io.pravega.connectors.flink.utils.SetupUtils;
import io.pravega.connectors.flink.utils.SuccessException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.util.TestLogger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.pravega.connectors.flink.utils.TestUtils.readLines;
import static org.junit.Assert.assertEquals;

public class FlinkPravegaDynamicTableITCase extends TestLogger {
    /**
     * Setup utility
     */
    private static final SetupUtils SETUP_UTILS = new SetupUtils();

    @Rule
    public final Timeout globalTimeout = new Timeout(120, TimeUnit.SECONDS);

    @BeforeClass
    public static void setupPravega() throws Exception {
        SETUP_UTILS.startAllServices();
    }

    @AfterClass
    public static void tearDownPravega() throws Exception {
        SETUP_UTILS.stopAllServices();
    }

    @Test
    public void testPravegaSourceSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(
                env,
                EnvironmentSettings.newInstance()
                        // Watermark is only supported in blink planner
                        .useBlinkPlanner()
                        .inStreamingMode()
                        .build()
        );
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        // we have to use single parallelism,
        // because we will count the messages in sink to terminate the job
        env.setParallelism(1);

        final String stream = RandomStringUtils.randomAlphabetic(20);
        SETUP_UTILS.createTestStream(stream, 1);

        final String createTable = String.format(
                "CREATE TABLE pravega (" +
                        "  `computed-price` as price + 1.0," +
                        "  price decimal(38, 18)," +
                        "  currency string," +
                        "  log_date date," +
                        "  log_time time(3)," +
                        "  log_ts timestamp(3)," +
                        "  ts as log_ts + INTERVAL '1' SECOND," +
                        "  watermark for ts as ts" +
                        ") WITH (" +
                        "  'connector' = 'pravega'," +
                        "  'controller-uri' = '%s'," +
                        "  'scope' = '%s'," +
                        "  'security.auth-type' = '%s'," +
                        "  'security.auth-token' = '%s'," +
                        "  'security.validate-hostname' = '%s'," +
                        "  'security.trust-store' = '%s'," +
                        "  'scan.execution.type' = '%s'," +
                        "  'scan.streams' = '%s'," +
                        "  'sink.stream' = '%s'," +
                        "  'sink.routing-key.field.name' = 'currency'," +
                        "  'format' = 'json'" +
                        ")",
                SETUP_UTILS.getControllerUri().toString(),
                SETUP_UTILS.getScope(),
                SETUP_UTILS.getAuthType(),
                SETUP_UTILS.getAuthToken(),
                SETUP_UTILS.isEnableHostNameValidation(),
                SETUP_UTILS.getPravegaClientTrustStore(),
                "streaming",
                stream,
                stream);

        tEnv.executeSql(createTable);

        String initialValues = "INSERT INTO pravega " +
                "SELECT CAST(price AS DECIMAL(10, 2)), currency, " +
                " CAST(d AS DATE), CAST(t AS TIME(0)), CAST(ts AS TIMESTAMP(3)) " +
                "FROM (VALUES (2.02,'Euro','2019-12-12', '00:00:01', '2019-12-12 00:00:01.001001')," +
                "  (1.11,'US Dollar','2019-12-12', '00:00:02', '2019-12-12 00:00:02.002001')," +
                "  (50,'Yen','2019-12-12', '00:00:03', '2019-12-12 00:00:03.004001')," +
                "  (3.1,'Euro','2019-12-12', '00:00:04', '2019-12-12 00:00:04.005001')," +
                "  (5.33,'US Dollar','2019-12-12', '00:00:05', '2019-12-12 00:00:05.006001')," +
                "  (0,'DUMMY','2019-12-12', '00:00:10', '2019-12-12 00:00:10'))" +
                "  AS orders (price, currency, d, t, ts)";

        // Write stream, Block until data is ready or job finished
        tEnv.executeSql(initialValues).await();

        // ---------- Read stream from Pravega -------------------

        String query = "SELECT" +
                "  CAST(TUMBLE_END(ts, INTERVAL '5' SECOND) AS VARCHAR)," +
                "  CAST(MAX(log_date) AS VARCHAR)," +
                "  CAST(MAX(log_time) AS VARCHAR)," +
                "  CAST(MAX(ts) AS VARCHAR)," +
                "  COUNT(*)," +
                "  CAST(MAX(price) AS DECIMAL(10, 2)) " +
                "FROM pravega " +
                "GROUP BY TUMBLE(ts, INTERVAL '5' SECOND)";

        List<String> expected = Arrays.asList(
                "+I(2019-12-12 00:00:05.000,2019-12-12,00:00:03,2019-12-12 00:00:04.004,3,50.00)",
                "+I(2019-12-12 00:00:10.000,2019-12-12,00:00:05,2019-12-12 00:00:06.006,2,5.33)");

        waitingExpectedResults(query, tEnv, env, expected, false, Duration.ofSeconds(10));
    }

    @Test
    public void testPravegaSourceSinkWithMetadata() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(
                env,
                EnvironmentSettings.newInstance()
                        // Watermark is only supported in blink planner
                        .useBlinkPlanner()
                        .inStreamingMode()
                        .build()
        );
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        // we have to use single parallelism,
        // because we will count the messages in sink to terminate the job
        env.setParallelism(1);

        final String stream = RandomStringUtils.randomAlphabetic(20);
        SETUP_UTILS.createTestStream(stream, 1);

        final String createTable = String.format(
                "CREATE TABLE users (" +
                        "  name STRING," +
                        "  phone INT," +
                        // access Pravega's `event-pointer` metadata
                        "  event_pointer BYTES METADATA VIRTUAL," +
                        "  vip BOOLEAN" +
                        ") WITH (" +
                        "  'connector' = 'pravega'," +
                        "  'controller-uri' = '%s'," +
                        "  'scope' = '%s'," +
                        "  'security.auth-type' = '%s'," +
                        "  'security.auth-token' = '%s'," +
                        "  'security.validate-hostname' = '%s'," +
                        "  'security.trust-store' = '%s'," +
                        "  'scan.execution.type' = '%s'," +
                        "  'scan.streams' = '%s'," +
                        "  'sink.stream' = '%s'," +
                        "  'format' = 'json'" +
                        ")",
                SETUP_UTILS.getControllerUri().toString(),
                SETUP_UTILS.getScope(),
                SETUP_UTILS.getAuthType(),
                SETUP_UTILS.getAuthToken(),
                SETUP_UTILS.isEnableHostNameValidation(),
                SETUP_UTILS.getPravegaClientTrustStore(),
                "streaming",
                stream,
                stream);

        tEnv.executeSql(createTable);

        String initialValues = "INSERT INTO users VALUES" +
                "('jack', 888888, FALSE)," +
                "('pony', 10000, TRUE)," +
                "('yiming', 12345678, FALSE)";

        // Write stream, Block until data is ready or job finished
        tEnv.executeSql(initialValues).await();

        // ---------- Read metadata from Pravega -------------------

        String query = "SELECT * FROM users";

        List<String> expectedNull = Arrays.asList("", "", "");
        try {
            waitingExpectedResults(query, tEnv, env, expectedNull, false, Duration.ofSeconds(10));
        } catch (AssertionError ignored) {
            // assert the data later, since it contains random event pointer
        }

        // ---------- Assert everything is what we expected -------------------

        // test all rows have an additional event pointer field
        assertEquals(3, TestingSinkFunction.ROWS.stream()
                .filter(rowData -> rowData.getArity() == 4).count());

        // test that all rows returned is what we insert, except for the event pointer field as the value is unknown
        List<GenericRowData> expected = Arrays.asList(
                GenericRowData.of(StringData.fromString("jack"), 888888, null, false),
                GenericRowData.of(StringData.fromString("pony"), 10000, null, true),
                GenericRowData.of(StringData.fromString("yiming"), 12345678, null, false)
        );
        for (int i = 0; i < 3; ++i) {
            assertEquals(expected.get(i).getString(0).toString(), TestingSinkFunction.ROWS.get(i).getString(0).toString());
            assertEquals(expected.get(i).getInt(1), TestingSinkFunction.ROWS.get(i).getInt(1));
            assertEquals(expected.get(i).getBoolean(3), TestingSinkFunction.ROWS.get(i).getBoolean(3));
        }

        // create a reader via pravega
        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(SETUP_UTILS.getScope(), SETUP_UTILS.getClientConfig())) {
            readerGroupManager.createReaderGroup(
                    "group",
                    ReaderGroupConfig.builder().stream(Stream.of(SETUP_UTILS.getScope(), stream)).build());
        }
        final String readerGroupId = UUID.randomUUID().toString();
        EventStreamReader<TestUser> consumer = EventStreamClientFactory.withScope(SETUP_UTILS.getScope(), SETUP_UTILS.getClientConfig()).createReader(
                readerGroupId,
                "group",
                new JsonSerializer<>(TestUser.class),
                ReaderConfig.builder().build());
        // test that the data read from pravega are the same with the data read from flink
        for (RowData rowDataFromFlink : TestingSinkFunction.ROWS) {
            TestUser rowDataFromPravega = consumer.fetchEvent(EventPointer.fromBytes(ByteBuffer.wrap(rowDataFromFlink.getBinary(2))));
            assertEquals(rowDataFromPravega.getName(), rowDataFromFlink.getString(0).toString());
            assertEquals(rowDataFromPravega.getPhone(), rowDataFromFlink.getInt(1));
            assertEquals(rowDataFromPravega.getVip(), rowDataFromFlink.getBoolean(3));
        }
    }

    @Test
    public void testPravegaDebeziumChangelogSourceSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(
                env,
                EnvironmentSettings.newInstance()
                        // Watermark is only supported in blink planner
                        .useBlinkPlanner()
                        .inStreamingMode()
                        .build()
        );
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        // we have to use single parallelism,
        // because we will count the messages in sink to terminate the job
        env.setParallelism(1);

        final String stream = RandomStringUtils.randomAlphabetic(20);
        SETUP_UTILS.createTestStream(stream, 1);

        // ---------- Write the Debezium json into Pravega -------------------
        List<String> lines = readLines("debezium-data-schema-exclude.txt");
        DataStreamSource<String> content = env.fromCollection(lines);
        SerializationSchema<String> serSchema = new SimpleStringSchema();

        FlinkPravegaWriter<String> pravegaSink = FlinkPravegaWriter.<String>builder()
                .forStream(stream)
                .withPravegaConfig(SETUP_UTILS.getPravegaConfig())
                .withSerializationSchema(serSchema)
                .withEventRouter(event -> "fixedkey")
                .withWriterMode(PravegaWriterMode.ATLEAST_ONCE)
                .build();
        content.addSink(pravegaSink).setParallelism(1);
        env.execute();

        // ---------- Produce an event time stream into Pravega -------------------
        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source (" +
                                // TODO: Support format metadata
                                // https://github.com/pravega/flink-connectors/issues/534
                                // + " origin_ts TIMESTAMP(3) METADATA FROM 'value.ingestion-timestamp' VIRTUAL," // unused
                                // + " origin_table STRING METADATA FROM 'value.source.table' VIRTUAL,"
                                "  id INT NOT NULL," +
                                "  name STRING," +
                                "  description STRING," +
                                "  weight DECIMAL(10,3)" +
                                ") WITH (" +
                                "  'connector' = 'pravega'," +
                                "  'controller-uri' = '%s'," +
                                "  'scope' = '%s'," +
                                "  'security.auth-type' = '%s'," +
                                "  'security.auth-token' = '%s'," +
                                "  'security.validate-hostname' = '%s'," +
                                "  'security.trust-store' = '%s'," +
                                "  'scan.execution.type' = '%s'," +
                                "  'scan.streams' = '%s'," +
                                "  'sink.stream' = '%s'," +
                                "  'format' = 'debezium-json'" +
                                ")",
                        SETUP_UTILS.getControllerUri().toString(),
                        SETUP_UTILS.getScope(),
                        SETUP_UTILS.getAuthType(),
                        SETUP_UTILS.getAuthToken(),
                        SETUP_UTILS.isEnableHostNameValidation(),
                        SETUP_UTILS.getPravegaClientTrustStore(),
                        "streaming",
                        stream,
                        stream);
        String sinkDDL =
                "CREATE TABLE sink (" +
                        "  name STRING," +
                        "  weightSum DECIMAL(10,3)," +
                        "  PRIMARY KEY (name) NOT ENFORCED" +
                        ") WITH (" +
                        "  'connector' = 'values'," +
                        "  'sink-insert-only' = 'false'" +
                        ")";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        String query = "INSERT INTO sink " +
                "SELECT name, SUM(weight) " +
                "FROM debezium_source GROUP BY name";
        /*
         * Debezium captures change data on the `products` table:
         *
         * <pre>
         * CREATE TABLE products (
         *  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
         *  name VARCHAR(255),
         *  description VARCHAR(512),
         *  weight FLOAT
         * );
         * ALTER TABLE products AUTO_INCREMENT = 101;
         *
         * INSERT INTO products
         * VALUES (default,"scooter","Small 2-wheel scooter",3.14),
         *        (default,"car battery","12V car battery",8.1),
         *        (default,"12-pack drill bits","12-pack of drill bits with sizes ranging from #40 to #3",0.8),
         *        (default,"hammer","12oz carpenter's hammer",0.75),
         *        (default,"hammer","14oz carpenter's hammer",0.875),
         *        (default,"hammer","16oz carpenter's hammer",1.0),
         *        (default,"rocks","box of assorted rocks",5.3),
         *        (default,"jacket","water resistent black wind breaker",0.1),
         *        (default,"spare tire","24 inch spare tire",22.2);
         * UPDATE products SET description='18oz carpenter hammer' WHERE id=106;
         * UPDATE products SET weight='5.1' WHERE id=107;
         * INSERT INTO products VALUES (default,"jacket","water resistent white wind breaker",0.2);
         * INSERT INTO products VALUES (default,"scooter","Big 2-wheel scooter ",5.18);
         * UPDATE products SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;
         * UPDATE products SET weight='5.17' WHERE id=111;
         * DELETE FROM products WHERE id=111;
         *
         * > SELECT * FROM products;
         * +-----+--------------------+---------------------------------------------------------+--------+
         * | id  | name               | description                                             | weight |
         * +-----+--------------------+---------------------------------------------------------+--------+
         * | 101 | scooter            | Small 2-wheel scooter                                   |   3.14 |
         * | 102 | car battery        | 12V car battery                                         |    8.1 |
         * | 103 | 12-pack drill bits | 12-pack of drill bits with sizes ranging from #40 to #3 |    0.8 |
         * | 104 | hammer             | 12oz carpenter's hammer                                 |   0.75 |
         * | 105 | hammer             | 14oz carpenter's hammer                                 |  0.875 |
         * | 106 | hammer             | 18oz carpenter hammer                                   |      1 |
         * | 107 | rocks              | box of assorted rocks                                   |    5.1 |
         * | 108 | jacket             | water resistent black wind breaker                      |    0.1 |
         * | 109 | spare tire         | 24 inch spare tire                                      |   22.2 |
         * | 110 | jacket             | new water resistent white wind breaker                  |    0.5 |
         * +-----+--------------------+---------------------------------------------------------+--------+
         * </pre>
         */
        List<String> expected =
                Arrays.asList(
                        "scooter,3.140",
                        "car battery,8.100",
                        "12-pack drill bits,0.800",
                        "hammer,2.625",
                        "rocks,5.100",
                        "jacket,0.600",
                        "spare tire,22.200");

        TableResult tableResult = waitingExpectedResults(query, tEnv, env, expected, true, Duration.ofSeconds(10));

        // ------------- cleanup -------------------
        tableResult.getJobClient().get().cancel().get(); // stop the job
    }

    public static TableResult waitingExpectedResults(String query,
                                                     StreamTableEnvironment tEnv,
                                                     StreamExecutionEnvironment env,
                                                     List<String> expected,
                                                     boolean testValues,
                                                     Duration timeout) throws Exception {
        long now = System.currentTimeMillis();
        long stop = now + timeout.toMillis();
        Collections.sort(expected);
        TableResult tableResult = null;

        while (System.currentTimeMillis() < stop) {
            try {
                if (testValues) {
                    tableResult = tEnv.executeSql(query);
                } else {
                    DataStream<RowData> result = tEnv.toAppendStream(tEnv.sqlQuery(query), RowData.class);
                    TestingSinkFunction sink = new TestingSinkFunction(expected.size());
                    result.addSink(sink).setParallelism(1);
                    env.execute("Job_2");
                }
            } catch (Exception e) {
                // we have to use a specific exception to indicate the job is finished,
                // because the registered Kafka source is infinite.
                if (!(ExceptionUtils.getRootCause(e) instanceof SuccessException)) {
                    // re-throw
                    throw e;
                }
            }

            List<String> actual;
            if (testValues) {
                actual = TestValuesTableFactory.getResults("sink");
                Collections.sort(actual);
            } else {
                actual = TestingSinkFunction.ROWS.stream().map(Object::toString).sorted().collect(Collectors.toList());
            }

            if (expected.equals(actual)) {
                return tableResult;
            }

            // A batch read from Pravega may not return events that were recently written.
            // In this case, we simply retry the read portion of this test.
            Thread.sleep(100);
        }

        if (testValues) {
            // timeout, assert again
            List<String> actual = TestValuesTableFactory.getResults("sink");
            Collections.sort(actual);
            assertEquals(expected, actual);
        }

        return tableResult;
    }

    private static final class TestingSinkFunction implements SinkFunction<RowData> {

        private static final long serialVersionUID = 455430015321124493L;

        private static final List<RowData> ROWS = new ArrayList<>();

        private final int expectedSize;

        private TestingSinkFunction(int expectedSize) {
            this.expectedSize = expectedSize;
            ROWS.clear();
        }

        @Override
        public void invoke(RowData value, Context context) throws Exception {
            ROWS.add(value);
            if (ROWS.size() >= expectedSize) {
                // job finish
                throw new SuccessException();
            }
        }
    }

    private static class TestUser implements Serializable {
        private static final long serialVersionUID = 8241970228716425282L;
        private String name;
        private Integer phone;
        private Boolean vip;

        public void setName(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setPhone(int phone) {
            this.phone = phone;
        }

        public int getPhone() {
            return phone;
        }

        public void setVip(boolean vip) {
            this.vip = vip;
        }

        public boolean getVip() {
            return vip;
        }
    }

}
