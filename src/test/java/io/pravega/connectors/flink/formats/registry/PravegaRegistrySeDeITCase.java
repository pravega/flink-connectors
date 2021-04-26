/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.flink.formats.registry;

import io.pravega.client.stream.Serializer;
import io.pravega.connectors.flink.table.catalog.pravega.PravegaCatalog;
import io.pravega.connectors.flink.utils.SchemaRegistryUtils;
import io.pravega.connectors.flink.utils.SetupUtils;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.Assert.assertArrayEquals;

/** Intergration Test for Pravega Registry serialization and deserialization schema. */
@SuppressWarnings("checkstyle:StaticVariableName")
public class PravegaRegistrySeDeITCase {
    private static final String TEST_CATALOG_NAME = "mycatalog";
    private static final String TEST_STREAM = "stream";
    private static Schema schema = null;
    private static RowType rowType = null;
    private static TypeInformation<RowData> typeInfo = null;

    /** Setup utility */
    private static final SetupUtils SETUP_UTILS = new SetupUtils();
    private static final SchemaRegistryUtils SCHEMA_REGISTRY_UTILS =
            new SchemaRegistryUtils(SETUP_UTILS, SchemaRegistryUtils.DEFAULT_PORT);

    private static PravegaCatalog CATALOG = null;

    @BeforeClass
    public static void setupPravega() throws Exception {
        SETUP_UTILS.startAllServices();
        SCHEMA_REGISTRY_UTILS.setupServices();
        CATALOG = new PravegaCatalog(TEST_CATALOG_NAME, SETUP_UTILS.getScope(),
                SETUP_UTILS.getControllerUri().toString(), SCHEMA_REGISTRY_UTILS.getSchemaRegistryUri().toString());
        init();
        CATALOG.open();
    }

    @AfterClass
    public static void tearDownPravega() throws Exception {
        CATALOG.close();
        SETUP_UTILS.stopAllServices();
        SCHEMA_REGISTRY_UTILS.tearDownServices();
    }

    @Test
    public void testSerializeDeserialize() throws Exception {
        final GenericRecord record = new GenericData.Record(schema);
        record.put(0, true);
        record.put(1, (int) Byte.MAX_VALUE);
        record.put(2, (int) Short.MAX_VALUE);
        record.put(3, 33);
        record.put(4, 44L);
        record.put(5, 12.34F);
        record.put(6, 23.45);
        record.put(7, "hello avro");
        record.put(8, ByteBuffer.wrap(new byte[] {1, 2, 4, 5, 6, 7, 8, 12}));

        record.put(
                9, ByteBuffer.wrap(BigDecimal.valueOf(123456789, 6).unscaledValue().toByteArray()));

        List<Double> doubles = new ArrayList<>();
        doubles.add(1.2);
        doubles.add(3.4);
        doubles.add(567.8901);
        record.put(10, doubles);

        record.put(11, 18397);
        record.put(12, 10087);
        record.put(13, 1589530213123L);
        record.put(14, 1589530213122L);

        Map<String, Long> map = new HashMap<>();
        map.put("flink", 12L);
        map.put("avro", 23L);
        record.put(15, map);

        Map<String, Map<String, Integer>> map2map = new HashMap<>();
        Map<String, Integer> innerMap = new HashMap<>();
        innerMap.put("inner_key1", 123);
        innerMap.put("inner_key2", 234);
        map2map.put("outer_key", innerMap);
        record.put(16, map2map);

        List<Integer> list1 = Arrays.asList(1, 2, 3, 4, 5, 6);
        List<Integer> list2 = Arrays.asList(11, 22, 33, 44, 55);
        Map<String, List<Integer>> map2list = new HashMap<>();
        map2list.put("list1", list1);
        map2list.put("list2", list2);
        record.put(17, map2list);

        Map<String, String> map2 = new HashMap<>();
        map2.put("key1", null);
        record.put(18, map2);

        PravegaRegistryRowDataSerializationSchema serializationSchema =
                new PravegaRegistryRowDataSerializationSchema(rowType, SETUP_UTILS.getScope(),
                        TEST_STREAM, SCHEMA_REGISTRY_UTILS.getSchemaRegistryUri());
        PravegaRegistryRowDataDeserializationSchema deserializationSchema =
                new PravegaRegistryRowDataDeserializationSchema(rowType, typeInfo, SETUP_UTILS.getScope(),
                        TEST_STREAM, SCHEMA_REGISTRY_UTILS.getSchemaRegistryUri());

        SchemaRegistryClientConfig schemaRegistryClientConfig = SchemaRegistryClientConfig.builder()
                .schemaRegistryUri(SCHEMA_REGISTRY_UTILS.getSchemaRegistryUri())
                .build();
        SerializerConfig config = SerializerConfig.builder()
                .registryConfig(schemaRegistryClientConfig)
                .namespace(SETUP_UTILS.getScope())
                .groupId(TEST_STREAM)
                .build();
        Serializer<GenericRecord> serializer = SerializerFactory.avroSerializer(config, AvroSchema.ofRecord(schema));

        byte[] input = serializer.serialize(record).array();
        RowData rowData = deserializationSchema.deserialize(input);
        byte[] output = serializationSchema.serialize(rowData);

        assertArrayEquals(input, output);
    }

    private static void init() throws Exception {
        final DataType dataType =
                ROW(
                        FIELD("bool", BOOLEAN()),
                        FIELD("tinyint", TINYINT()),
                        FIELD("smallint", SMALLINT()),
                        FIELD("int", INT()),
                        FIELD("bigint", BIGINT()),
                        FIELD("float", FLOAT()),
                        FIELD("double", DOUBLE()),
                        FIELD("name", STRING()),
                        FIELD("bytes", BYTES()),
                        FIELD("decimal", DECIMAL(19, 6)),
                        FIELD("doubles", ARRAY(DOUBLE())),
                        FIELD("time", TIME(0)),
                        FIELD("date", DATE()),
                        FIELD("timestamp3", TIMESTAMP(3)),
                        FIELD("timestamp3_2", TIMESTAMP(3)),
                        FIELD("map", MAP(STRING(), BIGINT())),
                        FIELD("map2map", MAP(STRING(), MAP(STRING(), INT()))),
                        FIELD("map2array", MAP(STRING(), ARRAY(INT()))),
                        FIELD("nullEntryMap", MAP(STRING(), STRING()))).notNull();
        rowType = (RowType) dataType.getLogicalType();
        typeInfo = InternalTypeInfo.of(rowType);
        schema = AvroSchemaConverter.convertToSchema(rowType);
        SCHEMA_REGISTRY_UTILS.registerSchema(TEST_STREAM, AvroSchema.of(schema), SerializationFormat.Avro);
        SETUP_UTILS.createTestStream(TEST_STREAM, 3);
    }
}
