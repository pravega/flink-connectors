/**
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

package org.apache.flink.streaming.connectors.pravega;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.pravega.serialization.JsonRowSerializationSchema;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.types.Row;

import java.util.function.Function;

/**
 * An append-only table sink to emit a streaming table as a Pravega stream containing JSON-formatted events.
 *
 * @deprecated Use the {@link Pravega} descriptor along with schema and format descriptors to define {@link FlinkPravegaTableSink}
 * See {@link org.apache.flink.table.api.TableEnvironment#connect(ConnectorDescriptor)}for more details on descriptors.
 */
@Deprecated
public class FlinkPravegaJsonTableSink extends FlinkPravegaTableSink {
    private FlinkPravegaJsonTableSink(Function<TableSinkConfiguration, FlinkPravegaWriter<Row>> writerFactory,
                                      Function<TableSinkConfiguration, FlinkPravegaOutputFormat<Row>> outputFormatFactory) {
        super(writerFactory, outputFormatFactory);
    }

    @Override
    protected FlinkPravegaTableSink createCopy() {
        return new FlinkPravegaJsonTableSink(writerFactory, outputFormatFactory);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder for {@link FlinkPravegaJsonTableSink}.
     */
    public static class Builder extends AbstractTableSinkBuilder<Builder> {

        protected Builder builder() {
            return this;
        }

        @Override
        @SuppressWarnings("deprecation")
        protected SerializationSchema<Row> getSerializationSchema(String[] fieldNames) {
            return new JsonRowSerializationSchema(fieldNames);
        }

        /**
         * Builds the {@link FlinkPravegaJsonTableSink}.
         */
        public FlinkPravegaJsonTableSink build() {
            return new FlinkPravegaJsonTableSink(this::createSinkFunction, this::createOutputFormat);
        }
    }
}
