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
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import io.pravega.schemaregistry.serializer.json.schemas.JSONSchema;
import io.pravega.schemaregistry.serializer.shared.codec.Encoder;
import io.pravega.schemaregistry.serializer.shared.impl.AbstractSerializer;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonAutoDetect;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.PropertyAccessor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.BYTES;
import static org.apache.flink.table.api.DataTypes.DATE;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.FLOAT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.MULTISET;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.SMALLINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.TINYINT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/** Intergration Test for Pravega Registry serialization and deserialization schema. */
@SuppressWarnings("checkstyle:StaticVariableName")
public class PravegaRegistrySeDeITCase {
    private static final String TEST_CATALOG_NAME = "mycatalog";

    /** Avro fields */
    private static final String AVRO_TEST_STREAM = "stream1";
    private static Schema avroSchema = null;
    private static RowType avroRowType = null;
    private static TypeInformation<RowData> avroTypeInfo = null;

    /** Json fields */
    private static final String JSON_TEST_STREAM = "stream2";
    private static JSONSchema<JsonTest> jsonSchema = null;
    private static RowType jsonRowType = null;
    private static TypeInformation<RowData> jsonTypeInfo = null;
    private static DataType jsonDataType = null;

    private static final boolean FAIL_ON_MISSING_FIELD = false;
    private static final boolean IGNORE_PARSE_ERRORS = false;
    private static final TimestampFormat TIMESTAMP_FORMAT = TimestampFormat.SQL;

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
        initAvro();
        initJson();
        CATALOG.open();
    }

    @AfterClass
    public static void tearDownPravega() throws Exception {
        CATALOG.close();
        SETUP_UTILS.stopAllServices();
        SCHEMA_REGISTRY_UTILS.tearDownServices();
    }

    @Test
    public void testAvroSerializeDeserialize() throws Exception {
        final GenericRecord record = new GenericData.Record(avroSchema);
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
                new PravegaRegistryRowDataSerializationSchema(avroRowType, SETUP_UTILS.getScope(),
                        AVRO_TEST_STREAM, SCHEMA_REGISTRY_UTILS.getSchemaRegistryUri());
        serializationSchema.open(null);
        PravegaRegistryRowDataDeserializationSchema deserializationSchema =
                new PravegaRegistryRowDataDeserializationSchema(avroRowType, avroTypeInfo, SETUP_UTILS.getScope(),
                        AVRO_TEST_STREAM, SCHEMA_REGISTRY_UTILS.getSchemaRegistryUri(),
                        FAIL_ON_MISSING_FIELD, IGNORE_PARSE_ERRORS, TIMESTAMP_FORMAT);
        deserializationSchema.open(null);

        SchemaRegistryClientConfig schemaRegistryClientConfig = SchemaRegistryClientConfig.builder()
                .schemaRegistryUri(SCHEMA_REGISTRY_UTILS.getSchemaRegistryUri())
                .build();
        SerializerConfig config = SerializerConfig.builder()
                .registryConfig(schemaRegistryClientConfig)
                .namespace(SETUP_UTILS.getScope())
                .groupId(AVRO_TEST_STREAM)
                .build();
        Serializer<GenericRecord> serializer = SerializerFactory.avroSerializer(config, AvroSchema.ofRecord(avroSchema));

        byte[] input = serializer.serialize(record).array();
        RowData rowData = deserializationSchema.deserialize(input);
        byte[] output = serializationSchema.serialize(rowData);

        assertArrayEquals(input, output);
    }

    @Test
    public void testJsonDeserialize() throws Exception {
        byte tinyint = 'c';
        short smallint = 128;
        int intValue = 45536;
        float floatValue = 33.333F;
        long bigint = 1238123899121L;
        String name = "asdlkjasjkdla998y1122";
        byte[] bytes = new byte[1024];
        ThreadLocalRandom.current().nextBytes(bytes);
        BigDecimal decimal = new BigDecimal("123.456789");
        Double[] doubles = new Double[] {1.1, 2.2, 3.3};

        Map<String, Long> map = new HashMap<>();
        map.put("flink", 123L);

        Map<String, Integer> multiSet = new HashMap<>();
        multiSet.put("blink", 2);

        JsonTest jsonTest = new JsonTest(true, tinyint, smallint, intValue, bigint, floatValue, name, bytes,
                decimal, doubles, map, multiSet);

        SchemaRegistryClientConfig schemaRegistryClientConfig = SchemaRegistryClientConfig.builder()
                .schemaRegistryUri(SCHEMA_REGISTRY_UTILS.getSchemaRegistryUri())
                .build();
        SerializerConfig serializerConfig = SerializerConfig.builder()
                .registryConfig(schemaRegistryClientConfig)
                .namespace(SETUP_UTILS.getScope())
                .groupId(JSON_TEST_STREAM)
                .build();
        Serializer<JsonTest> serializer = new JsonSerializer(
                JSON_TEST_STREAM,
                SchemaRegistryClientFactory.withNamespace(SETUP_UTILS.getScope(), schemaRegistryClientConfig),
                jsonSchema,
                serializerConfig.getEncoder(),
                serializerConfig.isRegisterSchema(),
                serializerConfig.isWriteEncodingHeader());

        byte[] serializedJson = serializer.serialize(jsonTest).array();

        PravegaRegistryRowDataDeserializationSchema deserializationSchema =
                new PravegaRegistryRowDataDeserializationSchema(
                        jsonRowType, jsonTypeInfo, SETUP_UTILS.getScope(), JSON_TEST_STREAM,
                        SCHEMA_REGISTRY_UTILS.getSchemaRegistryUri(),
                        FAIL_ON_MISSING_FIELD, IGNORE_PARSE_ERRORS, TIMESTAMP_FORMAT);
        deserializationSchema.open(null);

        Row expected = new Row(12);
        expected.setField(0, true);
        expected.setField(1, tinyint);
        expected.setField(2, smallint);
        expected.setField(3, intValue);
        expected.setField(4, bigint);
        expected.setField(5, floatValue);
        expected.setField(6, name);
        expected.setField(7, bytes);
        expected.setField(8, decimal);
        expected.setField(9, doubles);
        expected.setField(10, map);
        expected.setField(11, multiSet);

        RowData rowData = deserializationSchema.deserialize(serializedJson);
        Row actual = convertToExternal(rowData, jsonDataType);
        assertEquals(expected, actual);
    }

    private static void initAvro() throws Exception {
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
        avroRowType = (RowType) dataType.getLogicalType();
        avroTypeInfo = InternalTypeInfo.of(avroRowType);
        avroSchema = AvroSchemaConverter.convertToSchema(avroRowType);
        SCHEMA_REGISTRY_UTILS.registerSchema(AVRO_TEST_STREAM, AvroSchema.of(avroSchema), SerializationFormat.Avro);
        SETUP_UTILS.createTestStream(AVRO_TEST_STREAM, 3);
    }

    private static void initJson() throws Exception {
        jsonDataType =
                ROW(
                        FIELD("boolVal", BOOLEAN()),
                        FIELD("tinyintVal", TINYINT()),
                        FIELD("smallintVal", SMALLINT()),
                        FIELD("intVal", INT()),
                        FIELD("bigintVal", BIGINT()),
                        FIELD("floatVal", FLOAT()),
                        FIELD("nameVal", STRING()),
                        FIELD("bytesVal", BYTES()),
                        FIELD("decimalVal", DECIMAL(9, 6)),
                        FIELD("doublesVal", ARRAY(DOUBLE())),
                        FIELD("mapVal", MAP(STRING(), BIGINT())),
                        FIELD("multiSetVal", MULTISET(STRING())));
        jsonRowType = (RowType) jsonDataType.getLogicalType();
        jsonTypeInfo = InternalTypeInfo.of(jsonRowType);

        jsonSchema = JSONSchema.of(JsonTest.class);
        SCHEMA_REGISTRY_UTILS.registerSchema(JSON_TEST_STREAM, jsonSchema, SerializationFormat.Json);
        SETUP_UTILS.createTestStream(JSON_TEST_STREAM, 3);
    }

    private static class JsonTest {
        public Boolean boolVal;
        public Byte tinyintVal;
        public Short smallintVal;
        public Integer intVal;
        public Long bigintVal;
        public Float floatVal;
        public String nameVal;
        public byte[] bytesVal;
        public BigDecimal decimalVal;
        public Double[] doublesVal;
        public Map<String, Long> mapVal;
        public Map<String, Integer> multiSetVal;

        JsonTest(Boolean v1, Byte v2, Short v3, Integer v4, Long v5, Float v6,
                 String v7, byte[] v8, BigDecimal v9, Double[] v10,
                 Map<String, Long> v11, Map<String, Integer> v12) {
            this.boolVal = v1;
            this.tinyintVal = v2;
            this.smallintVal = v3;
            this.intVal = v4;
            this.bigintVal = v5;
            this.floatVal = v6;
            this.nameVal = v7;
            this.bytesVal = v8;
            this.decimalVal = v9;
            this.doublesVal = v10;
            this.mapVal = v11;
            this.multiSetVal = v12;
        }
    }

    private static class JsonSerializer<T> extends AbstractSerializer<T> {
        private final ObjectMapper objectMapper;

        public JsonSerializer(String groupId, SchemaRegistryClient client, JSONSchema<T> schema,
                              Encoder encoder, boolean registerSchema, boolean encodeHeader) {
            super(groupId, client, schema, encoder, registerSchema, encodeHeader);
            objectMapper = new ObjectMapper();
            objectMapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
        }

        @Override
        protected void serialize(T var, SchemaInfo schemaInfo, OutputStream outputStream) throws IOException {
            objectMapper.writeValue(outputStream, var);
            outputStream.flush();
        }
    }

    @SuppressWarnings("unchecked")
    private static Row convertToExternal(RowData rowData, DataType dataType) {
        return (Row) DataFormatConverters.getConverterForDataType(dataType).toExternal(rowData);
    }
}
