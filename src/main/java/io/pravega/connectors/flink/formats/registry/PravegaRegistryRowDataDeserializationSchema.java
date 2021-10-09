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
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.util.SchemaRegistryUtils;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import io.pravega.schemaregistry.serializer.shared.impl.AbstractDeserializer;
import io.pravega.schemaregistry.serializer.shared.impl.EncodingCache;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.AvroToRowDataConverters;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonToRowDataConverters;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;
/**
 * Deserialization schema from Pravega Schema Registry to Flink Table/SQL internal data structure {@link RowData}.
 *
 * <p>Deserializes a <code>byte[]</code> message as a Pravega Schema Registry and reads the specified fields.
 *
 * <p>Failures during deserialization are forwarded as wrapped IOExceptions.
 */
public class PravegaRegistryRowDataDeserializationSchema implements DeserializationSchema<RowData> {
    private static final long serialVersionUID = 1L;

    /**
     * Row type to generate the runtime converter.
     */
    private final RowType rowType;

    /**
     * Type information describing the result type.
     */
    private final TypeInformation<RowData> typeInfo;

    /**
     * Namespace describing the current scope.
     */
    private final String namespace;

    /**
     * GroupId describing the current stream.
     */
    private final String groupId;

    /**
     * Serialization format for schema registry.
     */
    private SerializationFormat serializationFormat;

    /**
     * Schema registry client config.
     */
    private transient SchemaRegistryClientConfig schemaRegistryClientConfig;

    /**
     * Deserializer to deserialize <code>byte[]</code> message.
     */
    private transient Serializer deserializer;

    // --------------------------------------------------------------------------------------------
    // Json fields
    // --------------------------------------------------------------------------------------------

    /** Flag indicating whether to fail if a field is missing. */
    private final boolean failOnMissingField;

    /** Flag indicating whether to ignore invalid fields/rows (default: throw an exception). */
    private final boolean ignoreParseErrors;

    /** Timestamp format specification which is used to parse timestamp. */
    private final TimestampFormat timestampFormat;

    public PravegaRegistryRowDataDeserializationSchema(
            RowType rowType,
            TypeInformation<RowData> typeInfo,
            String groupId,
            PravegaConfig pravegaConfig,
            boolean failOnMissingField,
            boolean ignoreParseErrors,
            TimestampFormat timestampFormat
            ) {
        if (ignoreParseErrors && failOnMissingField) {
            throw new IllegalArgumentException(
                    "JSON format doesn't support failOnMissingField and ignoreParseErrors are both enabled.");
        }
        this.rowType = rowType;
        this.typeInfo = checkNotNull(typeInfo);
        this.namespace = pravegaConfig.getDefaultScope();
        this.groupId = groupId;
        this.schemaRegistryClientConfig = SchemaRegistryUtils.getSchemaRegistryClientConfig(pravegaConfig);
        this.failOnMissingField = failOnMissingField;
        this.ignoreParseErrors = ignoreParseErrors;
        this.timestampFormat = timestampFormat;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void open(InitializationContext context) throws Exception {
        SchemaRegistryClient schemaRegistryClient = SchemaRegistryClientFactory.withNamespace(namespace,
                schemaRegistryClientConfig);
        SerializerConfig config = SerializerConfig.builder()
                .registryConfig(schemaRegistryClientConfig)
                .namespace(namespace)
                .groupId(groupId)
                .build();
        serializationFormat = schemaRegistryClient.getGroupProperties(groupId).getSerializationFormat();

        switch (serializationFormat) {
            case Avro:
                AvroSchema<Object> schema = AvroSchema.of(AvroSchemaConverter.convertToSchema(rowType));
                deserializer = SerializerFactory.avroGenericDeserializer(config, schema);
                break;
            case Json:
                ObjectMapper objectMapper = new ObjectMapper();
                boolean hasDecimalType =
                        LogicalTypeChecks.hasNested(rowType, t -> t instanceof DecimalType);
                if (hasDecimalType) {
                    objectMapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
                }
                deserializer = new FlinkJsonGenericDeserializer(
                        groupId,
                        schemaRegistryClient,
                        config.getDecoders(),
                        new EncodingCache(groupId, schemaRegistryClient),
                        config.isWriteEncodingHeader(),
                        objectMapper);
                break;
            default:
                throw new NotImplementedException("Not supporting deserialization format");
        }
    }

    @Override
    public RowData deserialize(@Nullable byte[] message) throws IOException {
        if (message == null) {
            return null;
        }
        try {
            return convertToRowData(deserializeToObject(message));
        } catch (Exception e) {
            if (ignoreParseErrors) {
                return null;
            }
            throw new IOException("Failed to deserialize byte array.", e);
        }
    }

    public Object deserializeToObject(byte[] message) {
        return deserializer.deserialize(ByteBuffer.wrap(message));
    }

    public RowData convertToRowData(Object message) {
        Object o;
        switch (serializationFormat) {
            case Avro:
                AvroToRowDataConverters.AvroToRowDataConverter avroConverter =
                        AvroToRowDataConverters.createRowConverter(rowType);
                o = avroConverter.convert(message);
                break;
            case Json:
                JsonToRowDataConverters.JsonToRowDataConverter jsonConverter =
                        new JsonToRowDataConverters(failOnMissingField, ignoreParseErrors, timestampFormat)
                                .createConverter(checkNotNull(rowType));
                o = jsonConverter.convert((JsonNode) message);
                break;
            default:
                throw new NotImplementedException("Not supporting deserialization format");
        }
        return (RowData) o;
    }

    private static class FlinkJsonGenericDeserializer extends AbstractDeserializer<JsonNode> {
        private final ObjectMapper objectMapper;

        public FlinkJsonGenericDeserializer(String groupId, SchemaRegistryClient client,
                                            SerializerConfig.Decoders decoders, EncodingCache encodingCache,
                                            boolean encodeHeader, ObjectMapper objectMapper) {
            super(groupId, client, null, false, decoders, encodingCache, encodeHeader);
            this.objectMapper = objectMapper;
        }

        @Override
        public final JsonNode deserialize(InputStream inputStream,
                                          SchemaInfo writerSchemaInfo,
                                          SchemaInfo readerSchemaInfo) throws IOException {
            return objectMapper.readTree(inputStream);
        }
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return typeInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PravegaRegistryRowDataDeserializationSchema that = (PravegaRegistryRowDataDeserializationSchema) o;
        return failOnMissingField == that.failOnMissingField && ignoreParseErrors == that.ignoreParseErrors &&
                Objects.equals(rowType, that.rowType) && Objects.equals(typeInfo, that.typeInfo) &&
                Objects.equals(namespace, that.namespace) && Objects.equals(groupId, that.groupId) &&
                serializationFormat == that.serializationFormat &&
                timestampFormat == that.timestampFormat;
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowType, typeInfo, namespace, groupId, serializationFormat, schemaRegistryClientConfig,
                failOnMissingField, ignoreParseErrors, timestampFormat);
    }
}
