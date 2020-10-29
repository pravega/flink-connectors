/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.flink.serialization;

import com.google.protobuf.DynamicMessage;
import io.pravega.client.stream.Serializer;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import io.pravega.schemaregistry.serializer.json.schemas.JSONSchema;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.nio.ByteBuffer;

@Slf4j
public class DeserializerFromSchemaRegistry<T> implements Serializer<T>, Serializable {
    private final PravegaConfig pravegaConfig;
    private final String group;
    private final Class<T> tClass;

    private final Object lock = new Object[0];

    // the Pravega serializer
    private transient Serializer<T> serializer;

    public DeserializerFromSchemaRegistry(PravegaConfig pravegaConfig, String group, Class<T> tClass) {
        Preconditions.checkNotNull(pravegaConfig.getSchemaRegistryClientConfig().getSchemaRegistryUri());
        this.pravegaConfig = pravegaConfig;
        this.group = group;
        this.tClass = tClass;
    }

    private void initialize() {
        synchronized (lock) {
            SchemaRegistryClientConfig schemaRegistryClientConfig = pravegaConfig.getSchemaRegistryClientConfig();
            SerializationFormat format;

            try (SchemaRegistryClient schemaRegistryClient = SchemaRegistryClientFactory.withNamespace(
                    pravegaConfig.getDefaultScope(), schemaRegistryClientConfig)) {
                format = schemaRegistryClient.getLatestSchemaVersion(group, null)
                        .getSchemaInfo().getSerializationFormat();
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
                    if (GenericRecord.class.isAssignableFrom(tClass)) {
                        serializer = (Serializer<T>) SerializerFactory.avroGenericDeserializer(serializerConfig, null);
                    } else {
                        serializer = SerializerFactory.avroDeserializer(serializerConfig, AvroSchema.of(tClass));
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
