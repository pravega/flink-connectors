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

import io.pravega.connectors.flink.utils.SetupUtils;
import io.pravega.connectors.flink.utils.SuccessException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.TestLogger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
                "create table pravega (%n" +
                        "  `computed-price` as price + 1.0,%n" +
                        "  price decimal(38, 18),%n" +
                        "  currency string,%n" +
                        "  log_date date,%n" +
                        "  log_time time(3),%n" +
                        "  log_ts timestamp(3),%n" +
                        "  ts as log_ts + INTERVAL '1' SECOND,%n" +
                        "  watermark for ts as ts%n" +
                        ") with (%n" +
                        "  'connector' = 'pravega',%n" +
                        "  'controller-uri' = '%s',%n" +
                        "  'scope' = '%s',%n" +
                        "  'security.auth-type' = '%s',%n" +
                        "  'security.auth-token' = '%s',%n" +
                        "  'security.validate-hostname' = '%s',%n" +
                        "  'security.trust-store' = '%s',%n" +
                        "  'scan.execution.type' = '%s',%n" +
                        "  'scan.reader-group.name' = '%s',%n" +
                        "  'scan.streams' = '%s',%n" +
                        "  'sink.stream' = '%s',%n" +
                        "  'sink.routing-key.field.name' = 'currency',%n" +
                        "  'format' = 'json'%n" +
                        ")",
                SETUP_UTILS.getControllerUri().toString(),
                SETUP_UTILS.getScope(),
                SETUP_UTILS.getAuthType(),
                SETUP_UTILS.getAuthToken(),
                SETUP_UTILS.isEnableHostNameValidation(),
                SETUP_UTILS.getPravegaClientTrustStore(),
                "streaming",
                "group",
                stream,
                stream);

        tEnv.executeSql(createTable);

        String initialValues = "INSERT INTO pravega\n" +
                "SELECT CAST(price AS DECIMAL(10, 2)), currency, " +
                " CAST(d AS DATE), CAST(t AS TIME(0)), CAST(ts AS TIMESTAMP(3))\n" +
                "FROM (VALUES (2.02,'Euro','2019-12-12', '00:00:01', '2019-12-12 00:00:01.001001'), \n" +
                "  (1.11,'US Dollar','2019-12-12', '00:00:02', '2019-12-12 00:00:02.002001'), \n" +
                "  (50,'Yen','2019-12-12', '00:00:03', '2019-12-12 00:00:03.004001'), \n" +
                "  (3.1,'Euro','2019-12-12', '00:00:04', '2019-12-12 00:00:04.005001'), \n" +
                "  (5.33,'US Dollar','2019-12-12', '00:00:05', '2019-12-12 00:00:05.006001'), \n" +
                "  (0,'DUMMY','2019-12-12', '00:00:10', '2019-12-12 00:00:10'))\n" +
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

            if (expected.equals(TestingSinkFunction.ROWS)) {
                break;
            }
            // A batch read from Pravega may not return events that were recently written.
            // In this case, we simply retry the read portion of this test.
            log.info("Retrying read query. expected={}, actual={}", expected, TestingSinkFunction.ROWS);
        }
    }

    private static final class TestingSinkFunction implements SinkFunction<RowData> {

        private static final long serialVersionUID = 455430015321124493L;

        private static final List<String> ROWS = new ArrayList<>();

        private final int expectedSize;

        private TestingSinkFunction(int expectedSize) {
            this.expectedSize = expectedSize;
            ROWS.clear();
        }

        @Override
        public void invoke(RowData value, Context context) throws Exception {
            ROWS.add(value.toString());
            if (ROWS.size() >= expectedSize) {
                // job finish
                throw new SuccessException();
            }
        }
    }

}
