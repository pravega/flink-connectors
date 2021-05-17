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
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.avro.RowDataToAvroConverters;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

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

    /** Avro serialization schema. */
    private transient Schema schema;

    /** Runtime instance that performs the actual work. */
    private final RowDataToAvroConverters.RowDataToAvroConverter runtimeConverter;

    /** Serializer to serialize {@link GenericRecord} to <code>byte[]</code>. */
    private transient Serializer<GenericRecord> serializer;

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

    public PravegaRegistryRowDataSerializationSchema(
            RowType rowType,
            String namespace,
            String groupId,
            URI schemaRegistryURI) {
        this.rowType = rowType;
        this.runtimeConverter = RowDataToAvroConverters.createConverter(rowType);
        this.serializer = null;
        this.namespace = namespace;
        this.groupId = groupId;
        this.schemaRegistryURI = schemaRegistryURI;
    }

    @Override
    public byte[] serialize(RowData row) {
        if (serializer == null) {
            initializeSerializer();
        }
        try {
            return convertToByteArray(serializeToGenericRecord(row));
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize row.", e);
        }
    }

    public GenericRecord serializeToGenericRecord(RowData row) {
        return (GenericRecord) runtimeConverter.convert(schema, row);
    }

    public byte[] convertToByteArray(GenericRecord message) {
        return serializer.serialize(message).array();
    }

    public void initializeSerializer() {
        schema = AvroSchemaConverter.convertToSchema(rowType);
        synchronized (this) {
            SchemaRegistryClientConfig schemaRegistryClientConfig = SchemaRegistryClientConfig.builder()
                    .schemaRegistryUri(schemaRegistryURI)
                    .build();
            SerializerConfig config = SerializerConfig.builder()
                    .registryConfig(schemaRegistryClientConfig)
                    .namespace(namespace)
                    .groupId(groupId)
                    .build();
            serializer = SerializerFactory.avroSerializer(config, AvroSchema.ofRecord(schema));
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
        return Objects.equals(rowType, that.rowType) && Objects.equals(namespace, that.namespace) && Objects.equals(groupId, that.groupId) && Objects.equals(schemaRegistryURI, that.schemaRegistryURI);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowType, namespace, groupId, schemaRegistryURI);
    }
}
