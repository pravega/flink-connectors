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
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import io.pravega.schemaregistry.serializer.json.schemas.JSONSchema;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.util.Preconditions;

import java.nio.ByteBuffer;

public class DeserializerFromSchemaRegistry<T> extends AbstractSerializerFromSchemaRegistry<T> {

    private static final long serialVersionUID = 1L;

    private final Class<T> tClass;

    // the Pravega deserializer
    private transient Serializer<T> serializer;

    public DeserializerFromSchemaRegistry(PravegaConfig pravegaConfig, String group, Class<T> tClass) {
        super(pravegaConfig, group);
        this.tClass = tClass;
        this.serializer = null;
    }

    @SuppressWarnings("unchecked")
    protected void initialize() {
        super.open();
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
