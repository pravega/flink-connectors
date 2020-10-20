package io.pravega.connectors.flink.table.catalog.pravega.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import io.pravega.schemaregistry.serializer.json.schemas.JSONSchema;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

@Internal
public class PravegaSchemaUtils {

    private PravegaSchemaUtils() {
        // private
    }

    public static TableSchema schemaInfoToTableSchema(SchemaInfo schemaInfo) {

        SerializationFormat format = schemaInfo.getSerializationFormat();
        String schemaString;
        TypeInformation<Row> typeInformation = null;

        switch (format) {
            case Json:
                ObjectMapper objectMapper = new ObjectMapper();
                JSONSchema jsonSchema = JSONSchema.from(schemaInfo);

                try {
                    schemaString = objectMapper.writeValueAsString(jsonSchema.getSchema());
                } catch (JsonProcessingException e) {
                    throw new RuntimeException("Failed to write message schema.", e);
                }

                typeInformation = JsonRowSchemaConverter.convert(schemaString);
                break;
            case Avro:
                AvroSchema avroSchema = AvroSchema.from(schemaInfo);

                schemaString = avroSchema.getSchema().toString();
                typeInformation = AvroSchemaConverter.convertToTypeInfo(schemaString);
                break;

            default:
                throw new NotImplementedException("Not supporting serialization format");
        }

        return TableSchema.fromTypeInfo(typeInformation);
    }
}
