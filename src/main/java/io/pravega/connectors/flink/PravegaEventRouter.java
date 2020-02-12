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
 * The event router which is used to extract the routing key for pravega from the event.
 *
 * @param <T> The type of the event.
 */
public interface PravegaEventRouter<T> extends Serializable {
    /**
     * Fetch the routing key for the given event.
     *
     * @param event The type of the event.
     * @return  The routing key which will be used by the pravega writer.
     */
    String getRoutingKey(T event);
}
