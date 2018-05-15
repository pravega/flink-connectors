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

import lombok.extern.slf4j.Slf4j;
import org.junit.BeforeClass;

/**
 * Integration tests for {@link FlinkPravegaWriter} in secure mode
 */
@Slf4j
public class FlinkPravegaWriterEnableAuthITCase extends FlinkPravegaWriterITCase {

    @BeforeClass
    public static void setup() throws Exception {
        // enable Pravega authentication, but not TLS
        SETUP_UTILS.startSecureServices(true, false);
    }
}
