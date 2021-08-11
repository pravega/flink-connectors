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

package io.pravega.connectors.flink.serialization;

import com.google.protobuf.DynamicMessage;
import io.pravega.client.stream.Serializer;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.util.SchemaRegistryUtils;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import io.pravega.schemaregistry.serializer.json.schemas.JSONSchema;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.nio.ByteBuffer;

@Slf4j
public class DeserializerFromSchemaRegistry<T> implements Serializer<T>, Serializable {

    private static final long serialVersionUID = 1L;

    private final PravegaConfig pravegaConfig;
    private final String group;
    private final Class<T> tClass;

    // the Pravega serializer
    private transient Serializer<T> serializer;

    public DeserializerFromSchemaRegistry(PravegaConfig pravegaConfig, String group, Class<T> tClass) {
        Preconditions.checkNotNull(pravegaConfig.getSchemaRegistryUri());
        this.pravegaConfig = pravegaConfig;
        this.group = group;
        this.tClass = tClass;
        this.serializer = null;
    }

    @SuppressWarnings("unchecked")
    private void initialize() {
        synchronized (this) {
            SchemaRegistryClientConfig schemaRegistryClientConfig = SchemaRegistryUtils.getSchemaRegistryClientConfig(pravegaConfig);
            SerializationFormat format;

            try (SchemaRegistryClient schemaRegistryClient = SchemaRegistryClientFactory.withNamespace(
                    pravegaConfig.getDefaultScope(), schemaRegistryClientConfig)) {
                format = schemaRegistryClient.getGroupProperties(group).getSerializationFormat();
            } catch (Exception e) {
                log.error("Error while closing the schema registry client", e);
                throw new FlinkRuntimeException(e);
            }

            SerializerConfig serializerConfig = SerializerConfig.builder()
                    .namespace(pravegaConfig.getDefaultScope())
                    .groupId(group)
                    .registerSchema(false)
                    .registryConfig(schemaRegistryClientConfig)
                    .build();

            switch (format) {
                case Json:
                    serializer = SerializerFactory.jsonDeserializer(serializerConfig, JSONSchema.of(tClass));
                    break;
                case Avro:
                    Preconditions.checkArgument(IndexedRecord.class.isAssignableFrom(tClass));
                    if (SpecificRecordBase.class.isAssignableFrom(tClass)) {
                        serializer = SerializerFactory.avroDeserializer(serializerConfig, AvroSchema.of(tClass));
                    } else {
                        serializer = (Serializer<T>) SerializerFactory.avroGenericDeserializer(serializerConfig, null);
                    }
                    break;
                case Protobuf:
                    if (DynamicMessage.class.isAssignableFrom(tClass)) {
                        serializer = (Serializer<T>) SerializerFactory.protobufGenericDeserializer(serializerConfig, null);
                    } else {
                        throw new UnsupportedOperationException("Only support DynamicMessage in Protobuf");
                    }
                    break;
                default:
                    throw new NotImplementedException("Not supporting serialization format");
            }
        }
    }

    @Override
    public ByteBuffer serialize(T value) {
        throw new NotImplementedException("Not supporting serialize in Deserializer");
    }

    @Override
    public T deserialize(ByteBuffer serializedValue) {
        if (serializer == null) {
            initialize();
        }
        return serializer.deserialize(serializedValue);
    }
}
