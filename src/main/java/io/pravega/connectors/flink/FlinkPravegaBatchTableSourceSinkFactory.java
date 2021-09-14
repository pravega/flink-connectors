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

package io.pravega.connectors.flink;

import org.apache.flink.table.factories.BatchTableSinkFactory;
import org.apache.flink.table.factories.BatchTableSourceFactory;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;

import static io.pravega.connectors.flink.table.descriptors.Pravega.CONNECTOR_VERSION_VALUE;

/**
 * A batch table source factory implementation of {@link BatchTableSourceFactory} to access Pravega streams.
 *
 * @deprecated Please use the new Table API {@link io.pravega.connectors.flink.dynamic.table.FlinkPravegaDynamicTableFactory}
 */
@Deprecated
public class FlinkPravegaBatchTableSourceSinkFactory extends FlinkPravegaTableFactoryBase implements
        BatchTableSourceFactory<Row>, BatchTableSinkFactory<Row> {

    @Override
    public Map<String, String> requiredContext() {
        return getRequiredContext();
    }

    @Override
    public List<String> supportedProperties() {
        return getSupportedProperties();
    }

    @Override
    public BatchTableSource<Row> createBatchTableSource(Map<String, String> properties) {
        return createFlinkPravegaTableSource(properties);
    }

    @Override
    public BatchTableSink<Row> createBatchTableSink(Map<String, String> properties) {
        return createFlinkPravegaTableSink(properties);
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
