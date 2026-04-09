package io.dalobscura.connectors.spark.v3;

import io.dalobscura.connectors.client.DalObscuraPlannedPartition;
import io.dalobscura.connectors.client.DalObscuraPlannedRead;
import io.dalobscura.connectors.client.DalObscuraReadClientFactory;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;

public final class DalObscuraBatch implements Scan, Batch {
    private final DalObscuraConnectorOptions options;
    private final DalObscuraReadClientFactory clientFactory;
    private final StructType schema;
    private final DalObscuraPlannedRead plannedRead;

    public DalObscuraBatch(
            DalObscuraConnectorOptions options,
            DalObscuraReadClientFactory clientFactory,
            StructType schema,
            DalObscuraPlannedRead plannedRead) {
        this.options = options;
        this.clientFactory = clientFactory;
        this.schema = schema;
        this.plannedRead = plannedRead;
    }

    @Override
    public StructType readSchema() {
        return schema;
    }

    @Override
    public Batch toBatch() {
        return this;
    }

    @Override
    public String description() {
        return "dal-obscura[" + options.target() + "]";
    }

    @Override
    public InputPartition[] planInputPartitions() {
        List<InputPartition> partitions = new ArrayList<>();
        for (DalObscuraPlannedPartition partition : plannedRead.partitions()) {
            partitions.add(new DalObscuraInputPartition(partition, options));
        }
        return partitions.toArray(new InputPartition[0]);
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new PartitionReaderFactory() {
            @Override
            public PartitionReader<InternalRow> createReader(InputPartition partition) {
                throw new UnsupportedOperationException(
                        "Columnar partition reader is implemented in Task 5");
            }
        };
    }
}
