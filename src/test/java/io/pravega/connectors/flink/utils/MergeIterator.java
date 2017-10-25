/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.flink.utils;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Collectors;

/**
 * Merges the elements of several iterators in round-robin order.
 */
public class MergeIterator<T> implements Iterator<T> {

    private final ArrayDeque<Iterator<T>> queue;

    public MergeIterator(Iterator<T>... iters) {
        this(Arrays.asList(iters));
    }

    public MergeIterator(Collection<Iterator<T>> iters) {
        queue = new ArrayDeque<>(iters.stream().filter(Iterator::hasNext).collect(Collectors.toList()));
    }

    @Override
    public boolean hasNext() {
        return !queue.isEmpty();
    }

    @Override
    public T next() {
        Iterator<T> i = queue.remove();
        assert i.hasNext();
        T elem = i.next();
        if (i.hasNext()) {
            queue.add(i);
        }
        return elem;
    }
}
