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

import org.apache.flink.table.factories.BatchTableSinkFactory;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;

import static org.apache.flink.streaming.connectors.pravega.Pravega.CONNECTOR_VERSION_VALUE;

/**
 * A batch table sink factory implementation of {@link BatchTableSinkFactory} to access Pravega streams.
 */
public class FlinkPravegaBatchTableSinkFactory extends FlinkPravegaTableFactoryBase implements BatchTableSinkFactory<Row> {

    @Override
    public BatchTableSink<Row> createBatchTableSink(Map<String, String> properties) {
        return createFlinkPravegaTableSink(properties);
    }

    @Override
    public Map<String, String> requiredContext() {
        return getRequiredContext();
    }

    @Override
    public List<String> supportedProperties() {
        return getSupportedProperties();
    }

    @Override
    protected String getVersion() {
        return String.valueOf(CONNECTOR_VERSION_VALUE);
    }

    @Override
    protected boolean isStreamEnvironment() {
        return false;
    }
}
