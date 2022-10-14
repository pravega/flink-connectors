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

package io.pravega.connectors.flink.utils.runtime;

import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.TransactionalEventStreamWriter;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;

/**
 * A Schema Registry operator is used for operating Schema Registry instance.
 */
public class SchemaRegistryRuntimeOperator implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryRuntimeOperator.class);

    private final PravegaRuntimeOperator pravegaOperator;
    private final URI schemaRegistryUri;
    private final EventStreamClientFactory eventStreamClientFactory;

    public SchemaRegistryRuntimeOperator(PravegaRuntimeOperator pravegaOperator, String schemaRegistryUri) {
        this.pravegaOperator = pravegaOperator;
        this.schemaRegistryUri = URI.create(schemaRegistryUri);
        eventStreamClientFactory = EventStreamClientFactory.withScope(pravegaOperator.getScope(),
                pravegaOperator.getClientConfig());
    }

    /**
     * Create a stream writer for writing Integer events with serializer from Schema Registry.
     *
     * @param stream    Name of the test stream.
     * @param schema    Schema for the writer.
     * @param format    Serialization format for serializer.
     *
     * @return Stream writer instance.
     */
    public EventStreamWriter<Object> getWriter(String stream, Schema schema, SerializationFormat format) {
        return eventStreamClientFactory.createEventWriter(
                stream,
                getSerializerFromRegistry(stream, schema, format),
                EventWriterConfig.builder().build());
    }

    /**
     * Register the schema to the Schema Registry service.
     *
     * @param stream    Name of the test stream.
     * @param schema    Schema for the writer.
     * @param format    Serialization format for serializer.
     */
    public void registerSchema(String stream, Schema schema, SerializationFormat format) {
        SchemaRegistryClient client = SchemaRegistryClientFactory.withNamespace(pravegaOperator.getScope(),
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

    /**
     * Create a serializer for the schema.
     *
     * @param stream    Name of the test stream.
     * @param schema    Schema for the writer.
     * @param format    Serialization format for serializer.
     *
     * @return A Serializer Implementation that can be used in {@link EventStreamWriter} or
     * {@link TransactionalEventStreamWriter}.
     */
    @SuppressWarnings("unchecked")
    public Serializer<Object> getSerializerFromRegistry(String stream, Schema<?> schema, SerializationFormat format) {
        SchemaRegistryClientConfig registryConfig = SchemaRegistryClientConfig.builder()
                .schemaRegistryUri(schemaRegistryUri)
                .build();
        SerializerConfig serializerConfig = SerializerConfig.builder()
                .namespace(pravegaOperator.getScope())
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

    /** Return the Schema Registry URI for this Schema Registry runtime. */
    public URI getSchemaRegistryUri() {
        return schemaRegistryUri;
    }

    @Override
    public void close() throws IOException {
        if (eventStreamClientFactory != null) {
            eventStreamClientFactory.close();
        }
        if (pravegaOperator != null) {
            pravegaOperator.close();
        }
    }
}
