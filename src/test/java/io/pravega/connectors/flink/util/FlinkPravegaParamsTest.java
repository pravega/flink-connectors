/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.connectors.flink.util;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class FlinkPravegaParamsTest {
    @Test
    public void testDefaultControllerUri() throws IOException {
        FlinkPravegaParams helper = new FlinkPravegaParams(ParameterTool.fromMap(new HashMap<>()));
        assertEquals(URI.create("tcp://127.0.0.1:9090"), helper.getControllerUri());
    }

    @Test
    public void testControllerUri() throws IOException {
        FlinkPravegaParams helper = new FlinkPravegaParams(ParameterTool.fromArgs(
            new String[] { "--controller", "tcp://controller.pravega.l4lb.thisdcos.directory:9091"}
        ));
        assertEquals(URI.create("tcp://controller.pravega.l4lb.thisdcos.directory:9091"), helper.getControllerUri());
    }

    @Test
    public void testStreamParam() {
        String input = "testScope/exampleStream";
        FlinkPravegaParams helper = new FlinkPravegaParams(ParameterTool.fromArgs(
            new String[] { "--input", input}
        ));
        assertEquals(input, helper.getStreamFromParam("input", "default/value").toString());

        helper = new FlinkPravegaParams(ParameterTool.fromMap(new HashMap<>()));
        assertEquals("default/value", helper.getStreamFromParam("input", "default/value").toString());
    }
}
