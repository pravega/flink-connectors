/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.connectors.flink.utils;

import io.pravega.client.stream.EventPointer;

import java.io.Serializable;

public class IntegerWithEventPointer implements Serializable {
    public static final int END_OF_STREAM = -1;

    private int value;
    private byte[] eventPointerBytes;

    public IntegerWithEventPointer() {}

    public IntegerWithEventPointer(int value) {
        this.value = value;
        this.eventPointerBytes = null;
    }

    public int getValue() {
        return value;
    }

    public byte[] getEventPointerBytes() {
        return eventPointerBytes;
    }

    public void setEventPointer(EventPointer eventPointer) {
        this.eventPointerBytes = eventPointer.toBytes().array();
    }

    public boolean isEndOfStream() {
        return value == END_OF_STREAM;
    }
}
