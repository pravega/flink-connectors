/**
 * Copyright Pravega Authors.
 *
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
package io.pravega.connectors.flink.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.state.CheckpointListener;

import java.util.concurrent.atomic.AtomicReference;

/**
 * An identity MapFunction that calls an interface once it receives a notification
 * that a checkpoint has been completed.
 */
public class NotifyingMapper<T> implements MapFunction<T, T>, CheckpointListener {

    public static final AtomicReference<Runnable> TO_CALL_ON_COMPLETION = new AtomicReference<>();

    @Override
    public T map(T element) throws Exception {
        return element;
    }

    @Override
    public void notifyCheckpointComplete(long l) throws Exception {
        TO_CALL_ON_COMPLETION.get().run();
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {}
}