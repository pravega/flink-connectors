/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.flink.table.catalog.pravega.formats.pravega.registry;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class PravegaRegistryOptions {

    public static final ConfigOption<String> URL = ConfigOptions
            .key("uri")
            .stringType()
            .noDefaultValue()
            .withDescription("Required schema registry URI");

    public static final ConfigOption<String> NAMESPACE = ConfigOptions
            .key("namespace")
            .stringType()
            .noDefaultValue()
            .withDescription("Required namespace");

    public static final ConfigOption<String> GROUP_ID = ConfigOptions
            .key("group-id")
            .stringType()
            .noDefaultValue()
            .withDescription("Required groupID");
}
