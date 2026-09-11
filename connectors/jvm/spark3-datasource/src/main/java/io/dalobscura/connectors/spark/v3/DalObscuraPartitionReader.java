package io.dalobscura.connectors.spark.v3;

import io.dalobscura.connectors.client.DalObscuraReadClient;
import io.dalobscura.connectors.client.DalObscuraTicketStream;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public final class DalObscuraPartitionReader implements PartitionReader<ColumnarBatch> {
    private final DalObscuraReadClient client;
    private final DalObscuraTicketStream stream;
    private final ArrowColumnarBatchAdapter adapter;
    private ColumnarBatch currentBatch;
    private boolean closed;

    public DalObscuraPartitionReader(
            DalObscuraReadClient client, DalObscuraInputPartition partition) {
        this.client = client;
        DalObscuraTicketStream openedStream = null;
        ArrowColumnarBatchAdapter openedAdapter = null;
        try {
            openedStream =
                    client.openStream(
                            partition.plannedPartition(), partition.executorAuthProvider().resolve());
            openedAdapter = new ArrowColumnarBatchAdapter(partition.requiredSchema());
        } catch (RuntimeException | Error failure) {
            closeAfterConstructionFailure(openedAdapter, openedStream, client, failure);
            throw failure;
        }
        this.stream = openedStream;
        this.adapter = openedAdapter;
    }

    @Override
    public boolean next() {
        if (!stream.next()) {
            currentBatch = null;
            return false;
        }
        currentBatch = adapter.adapt(stream.root());
        return true;
    }

    @Override
    public ColumnarBatch get() {
        return currentBatch;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        RuntimeException failure = null;
        if (currentBatch != null) {
            try {
                currentBatch.close();
            } catch (RuntimeException closeFailure) {
                failure = closeFailure;
            }
            currentBatch = null;
        }
        failure = closeResource(adapter, failure);
        failure = closeResource(stream, failure);
        failure = closeResource(client, failure);
        if (failure != null) {
            throw failure;
        }
    }

    private static void closeAfterConstructionFailure(
            ArrowColumnarBatchAdapter adapter,
            DalObscuraTicketStream stream,
            DalObscuraReadClient client,
            Throwable failure) {
        closeAndSuppress(adapter, failure);
        closeAndSuppress(stream, failure);
        closeAndSuppress(client, failure);
    }

    private static RuntimeException closeResource(AutoCloseable resource, RuntimeException failure) {
        try {
            resource.close();
        } catch (RuntimeException closeFailure) {
            if (failure == null) {
                return closeFailure;
            }
            failure.addSuppressed(closeFailure);
        } catch (Exception closeFailure) {
            RuntimeException wrapped = new RuntimeException(closeFailure);
            if (failure == null) {
                return wrapped;
            }
            failure.addSuppressed(wrapped);
        }
        return failure;
    }

    private static void closeAndSuppress(AutoCloseable resource, Throwable failure) {
        if (resource == null) {
            return;
        }
        try {
            resource.close();
        } catch (Exception closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }
}
