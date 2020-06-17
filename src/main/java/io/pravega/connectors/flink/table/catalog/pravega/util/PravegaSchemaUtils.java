package io.pravega.connectors.flink.table.catalog.pravega.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.pravega.schemaregistry.contract.data.SchemaInfo;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.schemas.JSONSchema;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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
    public static TableSchema schemaInfoToTableSchema(SchemaInfo schemaInfo) {

        // TODO: we support only json now
        Preconditions.checkArgument(schemaInfo.getSerializationFormat() == SerializationFormat.Json);

        ObjectMapper objectMapper = new ObjectMapper();
        JSONSchema jsonSchema = JSONSchema.from(schemaInfo);
        String schemaString;

        try {
            schemaString = objectMapper.writeValueAsString(jsonSchema.getSchema());
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to write message schema.", e);
        }

        TypeInformation<Row> typeInformation = JsonRowSchemaConverter.convert(schemaString);

        return TableSchema.fromTypeInfo(typeInformation);
    }
}
