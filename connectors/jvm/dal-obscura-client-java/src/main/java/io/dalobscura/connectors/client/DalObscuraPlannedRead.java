package io.dalobscura.connectors.client;

import java.util.List;
import org.apache.arrow.vector.types.pojo.Schema;

public final class DalObscuraPlannedRead {
    private final Schema schema;
    private final List<DalObscuraPlannedPartition> partitions;

    public DalObscuraPlannedRead(Schema schema, List<DalObscuraPlannedPartition> partitions) {
        this.schema = schema;
        this.partitions = partitions;
    }

    public Schema schema() {
        return schema;
    }

    public List<DalObscuraPlannedPartition> partitions() {
        return partitions;
    }
}
