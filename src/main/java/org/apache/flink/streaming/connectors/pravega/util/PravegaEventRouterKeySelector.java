/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.pravega.util;

import org.apache.flink.streaming.connectors.pravega.PravegaEventRouter;
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
