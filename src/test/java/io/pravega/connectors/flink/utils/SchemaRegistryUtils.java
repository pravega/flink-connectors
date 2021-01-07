/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.flink.utils;

import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import io.pravega.schemaregistry.serializer.json.schemas.JSONSchema;
import io.pravega.schemaregistry.serializer.protobuf.schemas.ProtobufSchema;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import io.pravega.schemaregistry.serializer.shared.schemas.Schema;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import io.pravega.schemaregistry.server.rest.RestServer;
import io.pravega.schemaregistry.server.rest.ServiceConfig;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import io.pravega.schemaregistry.storage.SchemaStore;
import io.pravega.schemaregistry.storage.SchemaStoreFactory;

import java.net.URI;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class SchemaRegistryUtils {

    public static final int DEFAULT_PORT = 10092;
    private SetupUtils setupUtils;
    private int port;
    private ScheduledExecutorService executor;
    private RestServer restServer;
    private URI schemaRegistryUri;

    private EventStreamClientFactory eventStreamClientFactory;

    public SchemaRegistryUtils(SetupUtils setupUtils, int port) {
        this.setupUtils = setupUtils;
        this.port = port;
        schemaRegistryUri = URI.create("http://localhost:" + port);
    }

    /**
     * Start Pravega schema registry services required for the test deployment.
     */
    public void setupServices() {
        executor = Executors.newScheduledThreadPool(10);

        ServiceConfig serviceConfig = ServiceConfig.builder().port(port).build();
        SchemaStore store = SchemaStoreFactory.createInMemoryStore(executor);

        SchemaRegistryService service = new SchemaRegistryService(store, executor);

        restServer = new RestServer(service, serviceConfig);
        restServer.startAsync();
        restServer.awaitRunning();
        eventStreamClientFactory = EventStreamClientFactory.withScope(setupUtils.getScope(), setupUtils.getClientConfig());
    }

    /**
     * Stop Pravega schema registry server and release all resources.
     */
    public void tearDownServices() {
        restServer.stopAsync();
        restServer.awaitTerminated();
        executor.shutdownNow();
        eventStreamClientFactory.close();
    }


    public URI getSchemaRegistryUri() {
        return schemaRegistryUri;
    }

    public EventStreamWriter<Object> getWriter(String stream, Schema schema, SerializationFormat format) {
        return eventStreamClientFactory.createEventWriter(
                stream,
                getSerializerFromRegistry(stream, schema, format),
                EventWriterConfig.builder().build());
    }

    public void registerSchema(String stream, Schema schema, SerializationFormat format) {
        SchemaRegistryClient client = SchemaRegistryClientFactory.withNamespace(setupUtils.getScope(),
                SchemaRegistryClientConfig.builder().schemaRegistryUri(schemaRegistryUri).build());
        client.addGroup(stream, new GroupProperties(format,
                Compatibility.allowAny(),
                true));

        client.addSchema(stream, schema.getSchemaInfo());
        try {
            client.close();
        } catch (Exception e) {

        }
    }

    @SuppressWarnings("unchecked")
    public Serializer<Object> getSerializerFromRegistry(String stream, Schema<?> schema, SerializationFormat format) {
        SchemaRegistryClientConfig registryConfig = SchemaRegistryClientConfig.builder()
                .schemaRegistryUri(schemaRegistryUri)
                .build();
        SerializerConfig serializerConfig = SerializerConfig.builder()
                .namespace(setupUtils.getScope())
                .groupId(stream)
                .registerSchema(false)
                .registryConfig(registryConfig)
                .build();

        switch (format) {
            case Json:
                return SerializerFactory.jsonSerializer(serializerConfig, (JSONSchema) schema);
            case Avro:
                return SerializerFactory.avroSerializer(serializerConfig, (AvroSchema) schema);
            case Protobuf:
                return SerializerFactory.protobufSerializer(serializerConfig, (ProtobufSchema) schema);
            default:
                return SerializerFactory.genericDeserializer(serializerConfig);
        }
    }
}
