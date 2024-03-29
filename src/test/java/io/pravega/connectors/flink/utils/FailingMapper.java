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
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * An identity mapper that throws an exception at a specified element.
 * The exception is only thrown during the first execution, prior to the first recovery.
 * <p>
 * <p>The function also fails, if the program terminates cleanly before the
 * function would throw an exception. That way, it guards against the case
 * where a failure is never triggered (for example because of a too high value for
 * the number of elements to pass before failing).
 */
public class FailingMapper<T> implements MapFunction<T, T>, ListCheckpointed<Integer> {
    private static final Logger LOG = LoggerFactory.getLogger(FailingMapper.class);

    /**
     * The number of elements to wait for, before failing
     */
    private final int failAtElement;

    private int elementCount;
    private boolean restored;

    /**
     * Creates a mapper that fails after processing the given number of elements.
     *
     * @param failAtElement The number of elements to wait for, before failing.
     */
    public FailingMapper(int failAtElement) {
        this.failAtElement = failAtElement;
    }

    @Override
    public T map(T element) throws Exception {
        if (!restored && ++elementCount > failAtElement) {
            LOG.info("Introducing Failure");
            throw new IntentionalException("artificial test failure");
        }

        return element;
    }

    @Override
    public void restoreState(List<Integer> list) throws Exception {
        restored = true;
    }

    @Override
    public List<Integer> snapshotState(long l, long l1) throws Exception {
        return null;
    }
}
