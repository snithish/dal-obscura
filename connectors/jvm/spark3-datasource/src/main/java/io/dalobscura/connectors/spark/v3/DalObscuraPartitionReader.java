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

    public DalObscuraPartitionReader(
            DalObscuraReadClient client, DalObscuraInputPartition partition) {
        this.client = client;
        this.stream = client.openStream(partition.plannedPartition(), partition.options().authToken());
        this.adapter = new ArrowColumnarBatchAdapter(partition.requiredSchema());
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
        if (currentBatch != null) {
            currentBatch.close();
            currentBatch = null;
        }
        adapter.close();
        stream.close();
        client.close();
    }
}
