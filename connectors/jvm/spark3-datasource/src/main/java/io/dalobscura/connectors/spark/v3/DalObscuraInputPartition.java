package io.dalobscura.connectors.spark.v3;

import io.dalobscura.connectors.client.DalObscuraPlannedPartition;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.types.StructType;

public final class DalObscuraInputPartition implements InputPartition {
    private final DalObscuraPlannedPartition plannedPartition;
    private final DalObscuraConnectorOptions options;
    private final StructType requiredSchema;

    public DalObscuraInputPartition(
            DalObscuraPlannedPartition plannedPartition,
            DalObscuraConnectorOptions options,
            StructType requiredSchema) {
        this.plannedPartition = plannedPartition;
        this.options = options;
        this.requiredSchema = requiredSchema;
    }

    public DalObscuraPlannedPartition plannedPartition() {
        return plannedPartition;
    }

    public DalObscuraConnectorOptions options() {
        return options;
    }

    public StructType requiredSchema() {
        return requiredSchema;
    }

    @Override
    public String[] preferredLocations() {
        return plannedPartition.locations().toArray(new String[0]);
    }
}
