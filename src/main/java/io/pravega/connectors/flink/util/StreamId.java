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

import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

/**
 * Captures the fully qualified name of a stream. The convention to represent this as a
 * single string is using [scope]/[stream].
 *
 * @deprecated Use {@link io.pravega.client.stream.Stream}
 */
@Deprecated
public class StreamId {
    public static final char STREAM_SPEC_SEPARATOR = '/';

    private String scope;
    private String name;

    public StreamId(String scope, String name) {
        this.scope = scope;
        this.name = name;
    }

    public String getScope() {
        return scope;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        StreamId streamId = (StreamId) o;
        return Objects.equals(scope, streamId.scope) &&
                Objects.equals(name, streamId.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scope, name);
    }

    public String getName() {
        return name;
    }

    /**
     * Creates a StreamId from a stream specification (
     * ).
     *
     * @param streamSpec StreamId
     */
    public static StreamId fromSpec(String streamSpec) {
        String[] parts = StringUtils.split(streamSpec, STREAM_SPEC_SEPARATOR);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Stream spec must be in the form [scope]/[stream]");
        }
        return new StreamId(parts[0], parts[1]);
    }

    @Override
    public String toString() {
        return scope + STREAM_SPEC_SEPARATOR + name;
    }
}
