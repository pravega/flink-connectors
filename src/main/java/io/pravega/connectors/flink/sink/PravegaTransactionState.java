package io.pravega.connectors.flink.sink;

import io.pravega.client.stream.Transaction;
import io.pravega.connectors.flink.PravegaEventRouter;

import java.util.Objects;

public class PravegaTransactionState<T> {
    private transient Transaction<T> transaction;
    private String transactionId;
    private Long watermark;

    private String writerId;

    PravegaTransactionState() {
        this(null, (Long) null);
    }

    PravegaTransactionState(Transaction<T> transaction, String writerId) {
        this(transaction, (Long) null);
        this.writerId = writerId;
    }

    PravegaTransactionState(Transaction<T> transaction, Long watermark) {
        this.transaction = transaction;
        if (transaction != null) {
            this.transactionId = transaction.getTxnId().toString();
        }
        this.watermark = watermark;
    }

    PravegaTransactionState(String transactionId, String writerId, Long watermark) {
        this.transactionId = transactionId;
        this.watermark = watermark;
        this.writerId = writerId;
    }

    public static <I> PravegaTransactionState of(FlinkPravegaInternalWriter<I> writer) {
        return new PravegaTransactionState(writer.getTransactionId(), writer.getWriterId(), writer.getCurrentWatermark());
    }

    public Transaction<T> getTransaction() {
        return transaction;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public Long getWatermark() {
        return watermark;
    }

    public String getWriterId() {
        return writerId;
    }

    public void setWatermark(Long watermark) {
        this.watermark = watermark;
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
