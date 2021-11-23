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
package io.pravega.connectors.flink.sink;

import java.util.Objects;

/**
 * This class holds the necessary information to construct a new {@link FlinkPravegaInternalWriter}
 * to commit transactions in {@link PravegaCommitter}.
 */
public class PravegaTransactionState {
    private final String transactionId;

    PravegaTransactionState(String transactionId) {
        this.transactionId = transactionId;
    }

    public static <I> PravegaTransactionState of(FlinkPravegaInternalWriter<I> writer) {
        return new PravegaTransactionState(writer.getTransactionId());
    }

    public static <I> PravegaTransactionState of(PravegaTransactionWriter<I> writer) {
        return new PravegaTransactionState(writer.getTransactionId());
    }

    public String getTransactionId() {
        return transactionId;
    }

    @Override
    public String toString() {
        return String.format(
                "%s [transactionId=%s]",
                this.getClass().getSimpleName(), transactionId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PravegaTransactionState that = (PravegaTransactionState) o;
        return Objects.equals(transactionId, that.transactionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionId);
    }
}
