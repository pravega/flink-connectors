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

import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE_VALUE_APPEND;

public class FlinkPravegaStreamTableSourceFactory extends FlinkPravegaTableSourceFactoryBase implements StreamTableSourceFactory<Row> {

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = getRequiredContext();
        context.put(UPDATE_MODE(), UPDATE_MODE_VALUE_APPEND());
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        return getSupportedProperties();
    }

    @Override
    public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
        return createFlinkPravegaTableSource(properties);
    }
}
