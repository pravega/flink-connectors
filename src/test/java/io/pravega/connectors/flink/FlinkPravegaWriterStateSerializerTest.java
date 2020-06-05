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

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction.TransactionHolder;

import java.util.Collections;
import java.util.Optional;

public class FlinkPravegaWriterStateSerializerTest extends SerializerTestBase<
        TwoPhaseCommitSinkFunction.State<FlinkPravegaWriter.PravegaTransactionState, Void>> {

    @Override
    protected TypeSerializer<TwoPhaseCommitSinkFunction.State<FlinkPravegaWriter.PravegaTransactionState, Void>> createSerializer() {
        return new TwoPhaseCommitSinkFunction.StateSerializer<>(
                new FlinkPravegaWriter.TransactionStateSerializer(), VoidSerializer.INSTANCE);
    }

    @Override
    protected Class<TwoPhaseCommitSinkFunction.State<FlinkPravegaWriter.PravegaTransactionState, Void>> getTypeClass() {
        return (Class) TwoPhaseCommitSinkFunction.State.class;
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected TwoPhaseCommitSinkFunction.State<FlinkPravegaWriter.PravegaTransactionState, Void>[] getTestData() {
        //noinspection unchecked
        return new TwoPhaseCommitSinkFunction.State[] {
                new TwoPhaseCommitSinkFunction.State<FlinkPravegaWriter.PravegaTransactionState, Void>(
                        new TransactionHolder(new FlinkPravegaWriter.PravegaTransactionState("txnid", 1L), 0),
                        Collections.emptyList(),
                        Optional.empty()),
                new TwoPhaseCommitSinkFunction.State<
                        FlinkPravegaWriter.PravegaTransactionState,
                        Void>(
                        new TransactionHolder(new FlinkPravegaWriter.PravegaTransactionState("txnid", 1L), 2711),
                        Collections.singletonList(new TransactionHolder(new FlinkPravegaWriter.PravegaTransactionState("txnid", 1L), 42)),
                        Optional.empty()),
                new TwoPhaseCommitSinkFunction.State<
                        FlinkPravegaWriter.PravegaTransactionState,
                        Void>(
                        new TransactionHolder(new FlinkPravegaWriter.PravegaTransactionState("txnid", 1L), 0),
                        Collections.singletonList(new TransactionHolder(new FlinkPravegaWriter.PravegaTransactionState("txnid", 1L), 0)),
                        Optional.empty()),
        };
    }

    @Override
    public void testInstantiate() {
        // this serializer does not support instantiation
    }
}
