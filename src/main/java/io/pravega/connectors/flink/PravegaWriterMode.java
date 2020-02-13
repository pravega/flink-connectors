/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.connectors.flink;

import java.io.Serializable;

/**
 * The supported modes of operation for flink's pravega writer.
 * The different modes correspond to different guarantees for the write operations that the implementation provides.
 */
public enum PravegaWriterMode implements Serializable {
    /*
     * Any write failures will be ignored hence there could be data loss.
     */
    BEST_EFFORT,

    /*
     * The writer will guarantee that all events are persisted in pravega.
     * There could be duplicate events written though.
     */
    ATLEAST_ONCE,

    /*
     * The writer will guarantee that all events are persisted in pravega exactly once.
     */
    EXACTLY_ONCE
}
