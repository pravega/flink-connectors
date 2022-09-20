/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import io.pravega.connectors.flink.utils.PravegaTestEnvironment;
import io.pravega.connectors.flink.utils.SuccessException;
import io.pravega.connectors.flink.utils.runtime.PravegaRuntime;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.pravega.connectors.flink.utils.TestUtils.readLines;
import static org.assertj.core.api.Assertions.assertThat;

@Timeout(value = 120)
public class FlinkPravegaDynamicTableITCase extends TestLogger {

    private static final PravegaTestEnvironment PRAVEGA = new PravegaTestEnvironment(PravegaRuntime.container());

    @BeforeAll
    public static void setupPravega() throws Exception {
        PRAVEGA.startUp();
    }

    @AfterAll
    public static void tearDownPravega() throws Exception {
        PRAVEGA.tearDown();
    }

    @Test
    public void testPravegaSourceSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(
                env,
                EnvironmentSettings.newInstance()
                        .inStreamingMode()
                        .build()
        );
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        // we have to use single parallelism,
        // because we will count the messages in sink to terminate the job
        env.setParallelism(1);

        final String stream = RandomStringUtils.randomAlphabetic(20);
        PRAVEGA.operator().createTestStream(stream, 1);

        final String createTable = String.format(
                "CREATE TABLE pravega ( %n" +
                        "  `computed-price` as price + 1.0, %n" +
                        "  price decimal(38, 18), %n" +
                        "  currency string, %n" +
                        "  log_date date, %n" +
                        "  log_time time(3), %n" +
                        "  log_ts timestamp(3), %n" +
                        "  ts as log_ts + INTERVAL '1' SECOND, %n" +
                        "  watermark for ts as ts %n" +
                        ") WITH ( %n" +
                        "  'connector' = 'pravega', %n" +
                        "  'controller-uri' = '%s', %n" +
                        "  'scope' = '%s', %n" +
                        "  'scan.execution.type' = '%s', %n" +
                        "  'scan.streams' = '%s', %n" +
                        "  'sink.stream' = '%s', %n" +
                        "  'sink.routing-key.field.name' = 'currency', %n" +
                        "  'format' = 'json' %n" +
                        ")",
                PRAVEGA.operator().getControllerUri().toString(),
                PRAVEGA.operator().getScope(),
                "streaming",
                stream,
                stream);

        tEnv.executeSql(createTable);

        String initialValues = "INSERT INTO pravega \n" +
                "SELECT CAST(price AS DECIMAL(10, 2)), currency, \n" +
                " CAST(d AS DATE), CAST(t AS TIME(0)), CAST(ts AS TIMESTAMP(3)) \n" +
                "FROM (VALUES (2.02,'Euro','2019-12-12', '00:00:01', '2019-12-12 00:00:01.001001'), \n" +
                "  (1.11,'US Dollar','2019-12-12', '00:00:02', '2019-12-12 00:00:02.002001'), \n" +
                "  (50,'Yen','2019-12-12', '00:00:03', '2019-12-12 00:00:03.004001'), \n" +
                "  (3.1,'Euro','2019-12-12', '00:00:04', '2019-12-12 00:00:04.005001'), \n" +
                "  (5.33,'US Dollar','2019-12-12', '00:00:05', '2019-12-12 00:00:05.006001'), \n" +
                "  (0,'DUMMY','2019-12-12', '00:00:10', '2019-12-12 00:00:10')) \n" +
                "  AS orders (price, currency, d, t, ts)";

        // Write stream, Block until data is ready or job finished
        tEnv.executeSql(initialValues).await();

        // ---------- Read stream from Pravega -------------------

        for (;;) {
            String query = "SELECT\n" +
                    "  CAST(TUMBLE_END(ts, INTERVAL '5' SECOND) AS VARCHAR),\n" +
                    "  CAST(MAX(log_date) AS VARCHAR),\n" +
                    "  CAST(MAX(log_time) AS VARCHAR),\n" +
                    "  CAST(MAX(ts) AS VARCHAR),\n" +
                    "  COUNT(*),\n" +
                    "  CAST(MAX(price) AS DECIMAL(10, 2))\n" +
                    "FROM pravega\n" +
                    "GROUP BY TUMBLE(ts, INTERVAL '5' SECOND)";

            DataStream<RowData> result = tEnv.toAppendStream(tEnv.sqlQuery(query), RowData.class);
            TestingSinkFunction sink = new TestingSinkFunction(2);
            result.addSink(sink).setParallelism(1);

            try {
                env.execute("Job_2");
            } catch (Exception e) {
                // we have to use a specific exception to indicate the job is finished,
                // because the registered Kafka source is infinite.
                if (!(ExceptionUtils.getRootCause(e) instanceof SuccessException)) {
                    // re-throw
                    throw e;
                }
            }

            List<String> expected = Arrays.asList(
                    "+I(2019-12-12 00:00:05.000,2019-12-12,00:00:03,2019-12-12 00:00:04.004,3,50.00)",
                    "+I(2019-12-12 00:00:10.000,2019-12-12,00:00:05,2019-12-12 00:00:06.006,2,5.33)");

            if (checkActualOutput(expected)) {
                break;
            }
            // A batch read from Pravega may not return events that were recently written.
            // In this case, we simply retry the read portion of this test.
            log.info("Retrying read query. expected={}, actual={}", expected, TestingSinkFunction.ROWS);
        }
    }

    static boolean checkActualOutput(List<String> expected) {
        return expected
                .equals(TestingSinkFunction.ROWS
                        .stream()
                        .map(Object::toString)
                        .collect(Collectors.toList()));
    }

    @Test
    public void testPravegaSourceSinkWithMetadata() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(
                env,
                EnvironmentSettings.newInstance()
                        .inStreamingMode()
                        .build()
        );
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        // we have to use single parallelism,
        // because we will count the messages in sink to terminate the job
        env.setParallelism(1);

        final String stream = RandomStringUtils.randomAlphabetic(20);
        PRAVEGA.operator().createTestStream(stream, 1);

        final String createTable = String.format(
                "CREATE TABLE users ( %n" +
                        "  name STRING, %n" +
                        "  phone INT, %n" +
                        // access Pravega's `event-pointer` metadata
                        "  event_pointer BYTES METADATA VIRTUAL, %n" +
                        "  vip BOOLEAN %n" +
                        ") WITH ( %n" +
                        "  'connector' = 'pravega', %n" +
                        "  'controller-uri' = '%s', %n" +
                        "  'scope' = '%s', %n" +
                        "  'scan.execution.type' = '%s', %n" +
                        "  'scan.streams' = '%s', %n" +
                        "  'sink.stream' = '%s', %n" +
                        "  'format' = 'json' %n" +
                        ")",
                PRAVEGA.operator().getControllerUri().toString(),
                PRAVEGA.operator().getScope(),
                "streaming",
                stream,
                stream);

        tEnv.executeSql(createTable);

        String initialValues = "INSERT INTO users VALUES \n" +
                "('jack', 888888, FALSE), \n" +
                "('pony', 10000, TRUE), \n" +
                "('yiming', 12345678, FALSE)";

        // Write stream, Block until data is ready or job finished
        tEnv.executeSql(initialValues).await();

        // ---------- Read metadata from Pravega -------------------

        String query = "SELECT * FROM users";

        DataStream<RowData> result = tEnv.toAppendStream(tEnv.sqlQuery(query), RowData.class);
        TestingSinkFunction sink = new TestingSinkFunction(3);
        result.addSink(sink).setParallelism(1);

        try {
            env.execute("Job_2");
        } catch (Exception e) {
            // we have to use a specific exception to indicate the job is finished,
            // because the registered Kafka source is infinite.
            if (!(ExceptionUtils.getRootCause(e) instanceof SuccessException)) {
                // re-throw
                throw e;
            }
        }

        // ---------- Assert everything is what we expected -------------------

        // test all rows have an additional event pointer field
        assertThat(TestingSinkFunction.ROWS.stream().filter(rowData -> rowData.getArity() == 4).count())
                .isEqualTo(3);

        // test that all rows returned is what we insert, except for the event pointer field as the value is unknown
        List<GenericRowData> expected = Arrays.asList(
                GenericRowData.of(StringData.fromString("jack"), 888888, null, false),
                GenericRowData.of(StringData.fromString("pony"), 10000, null, true),
                GenericRowData.of(StringData.fromString("yiming"), 12345678, null, false)
        );
        for (int i = 0; i < 3; ++i) {
            assertThat(TestingSinkFunction.ROWS.get(i).getString(0).toString())
                    .isEqualTo(expected.get(i).getString(0).toString());
            assertThat(TestingSinkFunction.ROWS.get(i).getInt(1))
                    .isEqualTo(expected.get(i).getInt(1));
            assertThat(TestingSinkFunction.ROWS.get(i).getBoolean(3))
                    .isEqualTo(expected.get(i).getBoolean(3));
        }

        // create a reader via pravega
        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(PRAVEGA.operator().getScope(), PRAVEGA.operator().getClientConfig())) {
            readerGroupManager.createReaderGroup(
                    "group",
                    ReaderGroupConfig.builder().stream(Stream.of(PRAVEGA.operator().getScope(), stream)).build());
        }
        final String readerGroupId = UUID.randomUUID().toString();
        EventStreamReader<TestUser> consumer = EventStreamClientFactory.withScope(PRAVEGA.operator().getScope(), PRAVEGA.operator().getClientConfig()).createReader(
                readerGroupId,
                "group",
                new JsonSerializer<>(TestUser.class),
                ReaderConfig.builder().build());
        // test that the data read from pravega are the same with the data read from flink
        for (RowData rowDataFromFlink : TestingSinkFunction.ROWS) {
            TestUser rowDataFromPravega = consumer.fetchEvent(EventPointer.fromBytes(ByteBuffer.wrap(rowDataFromFlink.getBinary(2))));
            assertThat(rowDataFromPravega.getName()).isEqualTo(rowDataFromFlink.getString(0).toString());
            assertThat(rowDataFromPravega.getPhone()).isEqualTo(rowDataFromFlink.getInt(1));
            assertThat(rowDataFromPravega.getVip()).isEqualTo(rowDataFromFlink.getBoolean(3));
        }
    }

    @Test
    public void testPravegaDebeziumChangelogSourceSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(
                env,
                EnvironmentSettings.newInstance()
                        .inStreamingMode()
                        .build()
        );
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        // we have to use single parallelism,
        // because we will count the messages in sink to terminate the job
        env.setParallelism(1);

        final String stream = RandomStringUtils.randomAlphabetic(20);
        PRAVEGA.operator().createTestStream(stream, 1);

        // ---------- Write the Debezium json into Pravega -------------------
        List<String> lines = readLines("debezium-data-schema-exclude.txt");
        DataStreamSource<String> content = env.fromCollection(lines);
        SerializationSchema<String> serSchema = new SimpleStringSchema();

        FlinkPravegaWriter<String> pravegaSink = FlinkPravegaWriter.<String>builder()
                .forStream(stream)
                .withPravegaConfig(PRAVEGA.operator().getPravegaConfig())
                .withSerializationSchema(serSchema)
                .withEventRouter(event -> "fixedkey")
                .withWriterMode(PravegaWriterMode.ATLEAST_ONCE)
                .build();
        content.addSink(pravegaSink).setParallelism(1);
        env.execute();

        // ---------- Produce an event time stream into Pravega -------------------
        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ( %n" +
                                // test format metadata
                                "  origin_ts TIMESTAMP(3) METADATA FROM 'from_format.ingestion-timestamp' VIRTUAL, %n" +  // unused
                                "  origin_table STRING METADATA FROM 'from_format.source.table' VIRTUAL, %n" +
                                "  id INT NOT NULL, %n" +
                                "  name STRING, %n" +
                                "  description STRING, %n" +
                                "  weight DECIMAL(10,3) %n" +
                                ") WITH ( %n" +
                                "  'connector' = 'pravega', %n" +
                                "  'controller-uri' = '%s', %n" +
                                "  'scope' = '%s', %n" +
                                "  'scan.execution.type' = '%s', %n" +
                                "  'scan.streams' = '%s', %n" +
                                "  'sink.stream' = '%s', %n" +
                                "  'format' = 'debezium-json' %n" +
                                ")",
                        PRAVEGA.operator().getControllerUri().toString(),
                        PRAVEGA.operator().getScope(),
                        "streaming",
                        stream,
                        stream);
        String sinkDDL =
                "CREATE TABLE sink ( \n" +
                        "  origin_table STRING, \n" +
                        "  name STRING, \n" +
                        "  weightSum DECIMAL(10,3), \n" +
                        "  PRIMARY KEY (name) NOT ENFORCED \n" +
                        ") WITH ( \n" +
                        "  'connector' = 'values', \n" +
                        "  'sink-insert-only' = 'false' \n" +
                        ")";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        TableResult tableResult =
                tEnv.executeSql(
                        "INSERT INTO sink "
                                + "SELECT FIRST_VALUE(origin_table), name, SUM(weight) "
                                + "FROM debezium_source GROUP BY name");
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
                        "+I[products, scooter, 3.140]",
                        "+I[products, car battery, 8.100]",
                        "+I[products, 12-pack drill bits, 0.800]",
                        "+I[products, hammer, 2.625]",
                        "+I[products, rocks, 5.100]",
                        "+I[products, jacket, 0.600]",
                        "+I[products, spare tire, 22.200]");

        waitingExpectedResults("sink", expected, Duration.ofSeconds(10));

        // ------------- cleanup -------------------
        tableResult.getJobClient().get().cancel().get(); // stop the job
    }

    public static void waitingExpectedResults(
            String sinkName, List<String> expected, Duration timeout) throws InterruptedException {
        long now = System.currentTimeMillis();
        long stop = now + timeout.toMillis();
        Collections.sort(expected);
        while (System.currentTimeMillis() < stop) {
            List<String> actual = TestValuesTableFactory.getResults(sinkName);
            Collections.sort(actual);
            if (expected.equals(actual)) {
                return;
            }
            Thread.sleep(100);
        }

        // timeout, assert again
        List<String> actual = TestValuesTableFactory.getResults(sinkName);
        Collections.sort(actual);
        assertThat(actual).isEqualTo(expected);
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
