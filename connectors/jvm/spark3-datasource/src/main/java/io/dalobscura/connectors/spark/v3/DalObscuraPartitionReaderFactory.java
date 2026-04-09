package io.dalobscura.connectors.spark.v3;

import io.dalobscura.connectors.client.DalObscuraReadClientFactory;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public final class DalObscuraPartitionReaderFactory implements PartitionReaderFactory {
    private final DalObscuraReadClientFactory clientFactory;

    public DalObscuraPartitionReaderFactory(DalObscuraReadClientFactory clientFactory) {
        this.clientFactory = clientFactory;
    }

    @Override
    public boolean supportColumnarReads(InputPartition partition) {
        return true;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        throw new UnsupportedOperationException("Only columnar reads are supported");
    }

    @Override
    public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
        return new DalObscuraPartitionReader(
                clientFactory.create(), (DalObscuraInputPartition) partition);
    }
}
