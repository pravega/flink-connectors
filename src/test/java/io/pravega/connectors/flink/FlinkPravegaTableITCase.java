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

import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.table.descriptors.Pravega;
import io.pravega.connectors.flink.utils.SetupUtils;
import io.pravega.connectors.flink.utils.SuccessException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.descriptors.ConnectTableDescriptor;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.factories.BatchTableSourceFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertTrue;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Integration tests for {@link FlinkPravegaTableSource}
 */
@Slf4j
public class FlinkPravegaTableITCase {

    // Setup utility.
    protected static final SetupUtils SETUP_UTILS = new SetupUtils();

    private static final List<Row> RESULTS = new ArrayList<>();

    // Number of events to generate for each of the tests.
    private static final int EVENT_COUNT_PER_SOURCE = 11;

    //Ensure each test completes within defined interval
    @Rule
    public final Timeout globalTimeout = new Timeout(180, TimeUnit.SECONDS);

    @BeforeClass
    public static void setup() throws Exception {
        SETUP_UTILS.startAllServices();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        SETUP_UTILS.stopAllServices();
    }

    @Test
    public void testTableSourceUsingDescriptor() throws Exception {
        StreamExecutionEnvironment execEnvWrite = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnvWrite.setParallelism(1);

        Stream stream = Stream.of(SETUP_UTILS.getScope(), "testJsonTableSource1");
        SETUP_UTILS.createTestStream(stream.getStreamName(), 1);

        // read data from the stream using Table reader
        TableSchema tableSchema = TableSchema.builder()
                .field("user", DataTypes.STRING())
                .field("uri", DataTypes.STRING())
                .field("accessTime", DataTypes.TIMESTAMP(3).bridgedTo(Timestamp.class))
                .build();
        TypeInformation<Row> typeInfo = (RowTypeInfo) TypeConversions.fromDataTypeToLegacyInfo(tableSchema.toRowDataType());

        PravegaConfig pravegaConfig = SETUP_UTILS.getPravegaConfig();

        // Write some data to the stream
        DataStreamSource<Row> dataStream = execEnvWrite
                .addSource(new TableEventSource(EVENT_COUNT_PER_SOURCE));

        FlinkPravegaWriter<Row> pravegaSink = FlinkPravegaWriter.<Row>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(stream)
                .withSerializationSchema(new JsonRowSerializationSchema.Builder(typeInfo).build())
                .withEventRouter((Row event) -> "fixedkey")
                .build();

        dataStream.addSink(pravegaSink);
        Assert.assertNotNull(execEnvWrite.getExecutionPlan());
        execEnvWrite.execute("PopulateRowData");

        testTableSourceStreamingDescriptor(stream, pravegaConfig);
        testTableSourceBatchDescriptor(stream, pravegaConfig);
    }

    private void testTableSourceStreamingDescriptor(Stream stream, PravegaConfig pravegaConfig) throws Exception {
        final StreamExecutionEnvironment execEnvRead = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnvRead.setParallelism(1);
        execEnvRead.enableCheckpointing(100);
        execEnvRead.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(execEnvRead,
                EnvironmentSettings.newInstance()
                        // watermark is only supported in blink planner
                        .useBlinkPlanner()
                        .inStreamingMode()
                        .build());
        RESULTS.clear();

        // read data from the stream using Table reader
        Schema schema = new Schema()
                .field("user", DataTypes.STRING())
                .field("uri", DataTypes.STRING())
                .field("accessTime", DataTypes.TIMESTAMP(3)).rowtime(
                        new Rowtime().timestampsFromField("accessTime").watermarksPeriodicBounded(30000L));

        Pravega pravega = new Pravega();
        pravega.tableSourceReaderBuilder()
                .withReaderGroupScope(stream.getScope())
                .forStream(stream)
                .withPravegaConfig(pravegaConfig);

        ConnectTableDescriptor desc = tableEnv.connect(pravega)
                .withFormat(new Json().failOnMissingField(true))
                .withSchema(schema)
                .inAppendMode();

        final Map<String, String> propertiesMap = desc.toProperties();
        final TableSource<?> source = TableFactoryService.find(StreamTableSourceFactory.class, propertiesMap)
                .createStreamTableSource(propertiesMap);

        String tableSourcePath = tableEnv.getCurrentDatabase() + "." + "MyTableRow";

        ConnectorCatalogTable<?, ?> connectorCatalogSourceTable = ConnectorCatalogTable.source(source, false);

        tableEnv.getCatalog(tableEnv.getCurrentCatalog()).get().createTable(
                ObjectPath.fromString(tableSourcePath),
                connectorCatalogSourceTable, false);

        String sqlQuery = "SELECT user, " +
                "TUMBLE_END(accessTime, INTERVAL '5' MINUTE) AS accessTime, " +
                "COUNT(uri) AS cnt " +
                "from MyTableRow GROUP BY " +
                "user, TUMBLE(accessTime, INTERVAL '5' MINUTE)";
        Table result = tableEnv.sqlQuery(sqlQuery);

        DataStream<Tuple2<Boolean, Row>> resultSet = tableEnv.toRetractStream(result, Row.class);
        StringSink2 stringSink = new StringSink2(8);
        resultSet.addSink(stringSink);

        try {
            execEnvRead.execute("ReadRowData");
        } catch (Exception e) {
            if (!(ExceptionUtils.getRootCause(e) instanceof SuccessException)) {
                throw e;
            }
        }

        log.info("results: {}", RESULTS);
        boolean compare = compare(RESULTS, getExpectedResultsAppend());
        assertTrue("Output does not match expected result", compare);
    }

    private void testTableSourceBatchDescriptor(Stream stream, PravegaConfig pravegaConfig) throws Exception {
        ExecutionEnvironment execEnvRead = ExecutionEnvironment.getExecutionEnvironment();
        // Can only use Legacy Flink planner for BatchTableEnvironment
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(execEnvRead);
        execEnvRead.setParallelism(1);

        Schema schema = new Schema()
                .field("user", DataTypes.STRING())
                .field("uri", DataTypes.STRING())
                // Note: LocalDateTime is not supported in legacy Flink planner, bridged to Timestamp with the data source.
                // See https://issues.apache.org/jira/browse/FLINK-16693 for more information.
                .field("accessTime", DataTypes.TIMESTAMP(3).bridgedTo(Timestamp.class));

        Pravega pravega = new Pravega();

        pravega.tableSourceReaderBuilder()
                .withReaderGroupScope(stream.getScope())
                .forStream(stream)
                .withPravegaConfig(pravegaConfig);

        ConnectTableDescriptor desc = tableEnv.connect(pravega)
                .withFormat(new Json().failOnMissingField(false))
                .withSchema(schema);

        final Map<String, String> propertiesMap = desc.toProperties();
        final TableSource<?> source = TableFactoryService.find(BatchTableSourceFactory.class, propertiesMap)
                .createBatchTableSource(propertiesMap);

        String tableSourcePath = tableEnv.getCurrentDatabase() + "." + "MyTableRow";

        ConnectorCatalogTable<?, ?> connectorCatalogSourceTable = ConnectorCatalogTable.source(source, true);

        tableEnv.getCatalog(tableEnv.getCurrentCatalog()).get().createTable(
                ObjectPath.fromString(tableSourcePath),
                connectorCatalogSourceTable, false);

        String sqlQuery = "SELECT user, count(uri) from MyTableRow GROUP BY user";

        Table result = tableEnv.sqlQuery(sqlQuery);

        DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);

        List<Row> results = resultSet.collect();
        log.info("results: {}", results);

        boolean compare = compare(results, getExpectedResultsRetracted());
        assertTrue("Output does not match expected result", compare);
    }

    static boolean compare(List<Row> results, List<Row> expectedResults) {

        if (results.size() != expectedResults.size()) {
            return false;
        }

        Set<Row> set = new HashSet<Row>(results);

        for (int i = 0; i < expectedResults.size(); i++) {
            Row expected = expectedResults.get(i);
            if (!set.contains(expected)) {
                return false;
            }
        }

        return true;
    }

    static List<Row> getExpectedResultsAppend() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.s");
        String[][] data = {
                { "Bob",   "2018-08-02 08:25:00.0", "1" },
                { "Chris", "2018-08-02 08:25:00.0", "1" },
                { "David", "2018-08-02 08:30:00.0", "1" },
                { "Gary",  "2018-08-02 09:35:00.0", "2" },
                { "Mary",  "2018-08-02 09:35:00.0", "2" },
                { "Nina",  "2018-08-02 09:40:00.0", "1" },
                { "Peter", "2018-08-02 09:45:00.0", "1" },
                { "Tony",  "2018-08-02 09:45:00.0", "1" },
                // The watermark is not passing the 10:45, so the window will not be fired and compute the count,
                // thus we don't have this line for output.
                //{ "Tony",  "2018-08-02 10:45:00.0", "1" },

        };
        List<Row> expectedResults = new ArrayList<>();
        for (int i = 0; i < data.length; i++) {
            Row row = new Row(3);
            row.setField(0, data[i][0]);
            row.setField(1, LocalDateTime.parse(data[i][1], formatter));
            row.setField(2, Long.valueOf(data[i][2]));
            expectedResults.add(row);
        }

        return expectedResults;
    }

    static List<Row> getExpectedResultsRetracted() {
        String[][] data = {
                { "Bob",   "1" },
                { "Chris", "1" },
                { "David", "1" },
                { "Gary",  "2" },
                { "Nina",  "1" },
                { "Mary",  "2" },
                { "Peter", "1" },
                { "Tony",  "2" }

        };

        List<Row> expectedResults = new ArrayList<>();

        for (int i = 0; i < data.length; i++) {
            Row row = new Row(2);
            row.setField(0, data[i][0]);
            row.setField(1, Long.valueOf(data[i][1]));
            expectedResults.add(row);
        }

        return expectedResults;
    }

    static class TableEventSource extends RichParallelSourceFunction<Row> {

        private final int totalEvents;

        private int currentOffset = 0;

        private final String[][] data = {
                {"Bob",   "/checkout",       "2018-08-02 08:20:00.0"},
                {"Chris", "/product?id=1",   "2018-08-02 08:23:00.0"},
                {"David", "/search",         "2018-08-02 08:25:00.0"},
                {"Gary",  "/cart",           "2018-08-02 09:32:00.0"},
                {"Mary",  "/checkout",       "2018-08-02 09:33:00.0"},
                {"Gary",  "/home",           "2018-08-02 09:33:00.0"},
                {"Nina",  "/cart",           "2018-08-02 09:39:00.0"},
                {"Mary",  "/search",         "2018-08-02 09:34:00.0"},
                {"Peter", "/checkout",       "2018-08-02 09:41:00.0"},
                {"Tony",  "/search",         "2018-08-02 09:41:00.0"},
                {"Tony",  "/cart",           "2018-08-02 10:41:00.0"}
        };

        public TableEventSource(final int totalEvents) {
            Preconditions.checkArgument(totalEvents > 0);
            this.totalEvents = totalEvents;
        }

        @Override
        public void run(SourceContext<Row> ctx) throws Exception {

            while (currentOffset < totalEvents) {
                Row row = new Row(3);
                row.setField(0, data[currentOffset][0]);
                row.setField(1, data[currentOffset][1]);
                row.setField(2, Timestamp.valueOf(data[currentOffset][2]));

                log.info("writing record: {}", row);
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(row);
                    currentOffset++;
                }
                Thread.sleep(100);
            }
        }

        @Override
        public void cancel() {
            // source terminates after fixed interval
        }
    }

    static class StringSink2 extends RichSinkFunction<Tuple2<Boolean, Row>> {

        private int eventCount;
        private int count = 0;

        public StringSink2(int eventCount) {
            this.eventCount = eventCount;
        }

        @Override
        public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
            log.info("reading row element: {}", value);
            Row data = value.f1;
            if (value.f0.booleanValue()) {
                log.info("adding row element: {}", value);
                RESULTS.add(data);
                count++;
            } else {
                int index = RESULTS.indexOf(data);
                if (index >= 0) {
                    RESULTS.remove(index);
                } else {
                    throw new RuntimeException("Tried to retract a value that wasn't added first. " +
                            "This is probably an incorrectly implemented test. " +
                            "Try to set the parallelism of the sink to 1.");
                }
            }

            if (count == eventCount) {
                log.info("done processing...");
                throw new SuccessException();
            }
        }

    }
}