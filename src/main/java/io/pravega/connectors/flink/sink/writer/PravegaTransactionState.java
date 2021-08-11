package io.pravega.connectors.flink.sink.writer;

import io.pravega.client.stream.Transaction;

import java.util.Objects;

public class PravegaTransactionState {
    private transient Transaction transaction;
    private String transactionId;
    private Long watermark;

    PravegaTransactionState() {
        this(null);
    }

    PravegaTransactionState(Transaction transaction) {
        this(transaction, null);
    }

    PravegaTransactionState(Transaction transaction, Long watermark) {
        this.transaction = transaction;
        if (transaction != null) {
            this.transactionId = transaction.getTxnId().toString();
        }
        this.watermark = watermark;
    }

    PravegaTransactionState(String transactionId, Long watermark) {
        this.transactionId = transactionId;
        this.watermark = watermark;
    }

    Transaction getTransaction() {
        return transaction;
    }

    @Override
    public String toString() {
        return String.format(
                "%s [transactionId=%s, watermark=%s]",
                this.getClass().getSimpleName(), transactionId, watermark);
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
        return Objects.equals(transactionId, that.transactionId) &&
                Objects.equals(watermark, that.watermark);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionId, watermark);
    }
}
