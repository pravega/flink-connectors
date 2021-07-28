/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.flink.source.split;

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;

/**
 * A {@link SourceSplit} implementation.
 *
 */
public class PravegaSplit implements SourceSplit, Serializable {

    private static final String PREFIX = "flink-reader";
    private int subtaskId;
    private String readerGroupName;

    /**
     * A Pravega Split instance represents an EventStreamReader, but with no operations and keep stateless.
     * To keep it serializable, we will not keep the reader inside.
     */
    public PravegaSplit(String readerGroupName, int subtaskId) {
        this.readerGroupName = readerGroupName;
        this.subtaskId = subtaskId;
    }

    public int getSubtaskId() {
        return subtaskId;
    }

    public String getReaderGroupName() {
        return readerGroupName;
    }

    @Override
    public String splitId() {
        // the splitId will be the Pravega reader Id.
        return splitId(subtaskId);
    }

    public static String splitId(int subtaskId) {
        return PREFIX + "-" + subtaskId;
    }
}
