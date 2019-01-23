/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.flink;

import org.apache.flink.table.factories.BatchTableSourceFactory;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;

public class FlinkPravegaBatchTableSourceFactory extends FlinkPravegaTableSourceFactoryBase implements BatchTableSourceFactory<Row> {

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
}
