/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.connectors.flink.util;

import io.pravega.connectors.flink.PravegaEventRouter;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * Implements a Flink {@link KeySelector} based on a Pravega {@link PravegaEventRouter}.  The event router
 * must implement {@link Serializable}.
 *
 * @param <T> The type of the event.
 */
class PravegaEventRouterKeySelector<T> implements KeySelector<T, String>, Serializable {

    private static final long serialVersionUID = 1L;

    private final PravegaEventRouter<T> eventRouter;

    /**
     * Creates a new key selector.
     *
     * @param eventRouter the event router to use.
     */
    public PravegaEventRouterKeySelector(PravegaEventRouter<T> eventRouter) {
        this.eventRouter = Preconditions.checkNotNull(eventRouter);
    }

    @Override
    public String getKey(T value) throws Exception {
        return this.eventRouter.getRoutingKey(value);
    }
}
