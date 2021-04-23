/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.flink.table.catalog.pravega.formats.pravega.registry;

import io.pravega.client.stream.Serializer;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.AvroToRowDataConverters;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Objects;

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
     * Type information describing the result type.
     */
    private final TypeInformation<RowData> typeInfo;

    /**
     * Deserializer to deserialize <code>byte[]</code> message to {@link Object}.
     */
    private transient Serializer<Object> genericDeserializer;

    /**
     * Namespace describing the current scope.
     */
    private String namespace;

    /**
     * GroupId describing the current stream.
     */
    private String groupId;

    /**
     * URI of schema registry.
     */
    private URI schemaRegistryURI;

    /**
     * Avro schema for avro generic deserializer.
     */
    private transient AvroSchema<Object> schema;

    /**
     * Runtime instance that performs the actual work.
     */
    private final AvroToRowDataConverters.AvroToRowDataConverter runtimeConverter;

    public PravegaRegistryRowDataDeserializationSchema(
            RowType rowType,
            TypeInformation<RowData> typeInfo,
            String namespace,
            String groupId,
            URI schemaRegistryURI) {
        this.typeInfo = typeInfo;
        this.runtimeConverter = AvroToRowDataConverters.createRowConverter(rowType);
        this.genericDeserializer = null;
        this.namespace = namespace;
        this.groupId = groupId;
        this.schemaRegistryURI = schemaRegistryURI;
        this.schema = AvroSchema.of(AvroSchemaConverter.convertToSchema(rowType));
    }

    @Override
    public RowData deserialize(@Nullable byte[] message) throws IOException {
        if (genericDeserializer == null) {
            initializeGenericDeserializer();
        }
        if (message == null) {
            return null;
        }
        try {
            return convertToRowData(deserializeToObject(message));
        } catch (Exception e) {
            throw new IOException("Failed to deserialize Avro record.", e);
        }
    }

    public Object deserializeToObject(byte[] message) {
        return genericDeserializer.deserialize(ByteBuffer.wrap(message));
    }

    public RowData convertToRowData(Object message) {
        return (RowData) runtimeConverter.convert(message);
    }

    public void initializeGenericDeserializer() {
        synchronized (this) {
            SchemaRegistryClientConfig schemaRegistryClientConfig = SchemaRegistryClientConfig.builder()
                    .schemaRegistryUri(schemaRegistryURI)
                    .build();
            SerializerConfig config = SerializerConfig.builder()
                    .registryConfig(schemaRegistryClientConfig)
                    .namespace(namespace)
                    .groupId(groupId)
                    .build();
            genericDeserializer = SerializerFactory.avroGenericDeserializer(config, schema);
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
        return Objects.equals(typeInfo, that.typeInfo) && Objects.equals(genericDeserializer, that.genericDeserializer) && Objects.equals(namespace, that.namespace) && Objects.equals(groupId, that.groupId) && Objects.equals(schemaRegistryURI, that.schemaRegistryURI);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeInfo, genericDeserializer, runtimeConverter);
    }
}
