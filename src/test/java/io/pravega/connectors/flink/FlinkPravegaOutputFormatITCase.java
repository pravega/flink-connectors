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
import io.pravega.connectors.flink.utils.IntegerDeserializationSchema;
import io.pravega.connectors.flink.utils.IntegerSerializationSchema;
import io.pravega.connectors.flink.utils.SetupUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

@Slf4j
public class FlinkPravegaOutputFormatITCase extends AbstractTestBase {

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
     * Verifies the following using DataSet API:
     *  - writes data into Pravega using {@link FlinkPravegaOutputFormat}.
     *  - reads data from Pravega using {@link FlinkPravegaInputFormat}.
     */
    @Test
    public void testPravegaOutputFormat() throws Exception {

        Stream stream = Stream.of(SETUP_UTILS.getScope(), "outputFormatDataSet");
        SETUP_UTILS.createTestStream(stream.getStreamName(), 1);

        PravegaConfig pravegaConfig = SETUP_UTILS.getPravegaConfig();

        FlinkPravegaOutputFormat<Integer> flinkPravegaOutputFormat = FlinkPravegaOutputFormat.<Integer>builder()
                .withEventRouter(router -> "fixedKey")
                .withSerializationSchema(new IntegerSerializationSchema())
                .withPravegaConfig(pravegaConfig)
                .forStream(stream)
                .build();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Collection<Integer> inputData = Arrays.asList(10, 20);
        env.fromCollection(inputData)
                .output(flinkPravegaOutputFormat);
        env.execute("write");

        DataSet<Integer> integers = env.createInput(
                FlinkPravegaInputFormat.<Integer>builder()
                        .forStream(stream)
                        .withPravegaConfig(SETUP_UTILS.getPravegaConfig())
                        .withDeserializationSchema(new IntegerDeserializationSchema())
                        .build(),
                BasicTypeInfo.INT_TYPE_INFO
        );

        // verify that all events were read
        Assert.assertEquals(2, integers.collect().size());
    }


}
