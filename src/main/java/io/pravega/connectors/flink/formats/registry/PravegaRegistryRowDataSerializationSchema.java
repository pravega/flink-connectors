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

package io.pravega.connectors.flink.formats.registry;

import io.pravega.client.stream.Serializer;
import io.pravega.connectors.flink.table.catalog.pravega.util.PravegaSchemaUtils;
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
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.avro.RowDataToAvroConverters;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.formats.json.JsonOptions;
import org.apache.flink.formats.json.RowDataToJsonConverters;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Objects;

/**
 * Serialization schema that serializes an object of Flink internal data structure {@link RowData} into
 * Pravega Schema Registry bytes.
 *
 * <p>Serializes the input Flink object into GenericRecord and converts it into <code>byte[]</code>.
 *
 * <p>Result <code>byte[]</code> messages can be deserialized using {@link
 * PravegaRegistryRowDataDeserializationSchema}.
 */
public class PravegaRegistryRowDataSerializationSchema implements SerializationSchema<RowData> {
    private static final long serialVersionUID = 1L;

    /** RowType to generate the runtime converter. */
    private final RowType rowType;

    /** Serializer to serialize to <code>byte[]</code>. */
    private transient Serializer serializer;

    /**
     * Namespace describing the current scope.
     */
    private final String namespace;

    /**
     * GroupId describing the current stream.
     */
    private final String groupId;

    /**
     * Schema Registry URI.
     */
    private final URI schemaRegistryURI;

    /**
     * Serialization format for schema registry.
     */
    private SerializationFormat serializationFormat;

    // --------------------------------------------------------------------------------------------
    // Avro fields
    // --------------------------------------------------------------------------------------------

    /** Avro serialization schema. */
    private transient Schema avroSchema;

    // --------------------------------------------------------------------------------------------
    // Json fields
    // --------------------------------------------------------------------------------------------

    /** Timestamp format specification which is used to parse timestamp. */
    private final TimestampFormat timestampFormat;

    /** The handling mode when serializing null keys for map data. */
    private final JsonOptions.MapNullKeyMode mapNullKeyMode;

    /** The string literal when handling mode for map null key LITERAL. is */
    private final String mapNullKeyLiteral;

    public PravegaRegistryRowDataSerializationSchema(
            RowType rowType,
            String namespace,
            String groupId,
            URI schemaRegistryURI,
            SerializationFormat serializationFormat,
            TimestampFormat timestampOption,
            JsonOptions.MapNullKeyMode mapNullKeyMode,
            String mapNullKeyLiteral) {
        this.rowType = rowType;
        this.serializer = null;
        this.namespace = namespace;
        this.groupId = groupId;
        this.schemaRegistryURI = schemaRegistryURI;
        this.serializationFormat = serializationFormat;
        this.timestampFormat = timestampOption;
        this.mapNullKeyMode = mapNullKeyMode;
        this.mapNullKeyLiteral = mapNullKeyLiteral;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void open(InitializationContext context) throws Exception {
        SchemaRegistryClientConfig schemaRegistryClientConfig = SchemaRegistryClientConfig.builder()
                .schemaRegistryUri(schemaRegistryURI)
                .build();
        SchemaRegistryClient schemaRegistryClient = SchemaRegistryClientFactory.withNamespace(namespace,
                schemaRegistryClientConfig);
        SerializerConfig config = SerializerConfig.builder()
                .registryConfig(schemaRegistryClientConfig)
                .namespace(namespace)
                .groupId(groupId)
                .build();

        switch (serializationFormat) {
            case Avro:
                avroSchema = AvroSchemaConverter.convertToSchema(rowType);
                serializer = SerializerFactory.avroSerializer(config, AvroSchema.ofRecord(avroSchema));
                break;
            case Json:
                String jsonSchemaString = PravegaSchemaUtils.convertToJsonSchemaString(rowType);
                serializer = new FlinkJsonSerializer(
                        groupId,
                        schemaRegistryClient,
                        JSONSchema.of("", jsonSchemaString, JsonNode.class),
                        config.getEncoder(),
                        config.isRegisterSchema(),
                        config.isWriteEncodingHeader());
                break;
            default:
                throw new NotImplementedException("Not supporting deserialization format");
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public byte[] serialize(RowData row) {
        try {
            switch (serializationFormat) {
                case Avro:
                    return convertToByteArray(serializeToGenericRecord(row));
                case Json:
                    return convertToByteArray(serializaToJsonNode(row));
                default:
                    throw new NotImplementedException("Not supporting deserialization format");
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize row.", e);
        }
    }

    public GenericRecord serializeToGenericRecord(RowData row) {
        RowDataToAvroConverters.RowDataToAvroConverter runtimeConverter =
                RowDataToAvroConverters.createConverter(rowType);
        return (GenericRecord) runtimeConverter.convert(avroSchema, row);
    }

    public JsonNode serializaToJsonNode(RowData row) {
        RowDataToJsonConverters.RowDataToJsonConverter runtimeConverter = new RowDataToJsonConverters(
                timestampFormat, mapNullKeyMode, mapNullKeyLiteral)
                .createConverter(rowType);
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        return runtimeConverter.convert(mapper, node, row);
    }

    @SuppressWarnings("unchecked")
    public byte[] convertToByteArray(Object message) {
        return serializer.serialize(message).array();
    }

    @VisibleForTesting
    protected static class FlinkJsonSerializer extends AbstractSerializer<JsonNode> {
        private final ObjectMapper objectMapper;
        public FlinkJsonSerializer(String groupId, SchemaRegistryClient client, JSONSchema schema,
                                   Encoder encoder, boolean registerSchema, boolean encodeHeader) {
            super(groupId, client, schema, encoder, registerSchema, encodeHeader);
            objectMapper = new ObjectMapper();
        }

        @Override
        protected void serialize(JsonNode jsonNode, SchemaInfo schemaInfo, OutputStream outputStream) throws IOException {
            objectMapper.writeValue(outputStream, jsonNode);
            outputStream.flush();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PravegaRegistryRowDataSerializationSchema that = (PravegaRegistryRowDataSerializationSchema) o;
        return Objects.equals(rowType, that.rowType) && Objects.equals(namespace, that.namespace) &&
                Objects.equals(groupId, that.groupId) && Objects.equals(schemaRegistryURI, that.schemaRegistryURI) &&
                serializationFormat == that.serializationFormat && timestampFormat == that.timestampFormat &&
                mapNullKeyMode == that.mapNullKeyMode && Objects.equals(mapNullKeyLiteral, that.mapNullKeyLiteral);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowType, namespace, groupId, schemaRegistryURI, serializationFormat,
                timestampFormat, mapNullKeyMode, mapNullKeyLiteral);
    }
}
