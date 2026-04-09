package io.dalobscura.connectors.spark.v3;

import io.dalobscura.connectors.client.DalObscuraPlannedPartition;
import org.apache.spark.sql.connector.read.InputPartition;

public final class DalObscuraInputPartition implements InputPartition {
    private final DalObscuraPlannedPartition plannedPartition;
    private final DalObscuraConnectorOptions options;

    public DalObscuraInputPartition(
            DalObscuraPlannedPartition plannedPartition, DalObscuraConnectorOptions options) {
        this.plannedPartition = plannedPartition;
        this.options = options;
    }

    public DalObscuraPlannedPartition plannedPartition() {
        return plannedPartition;
    }

    public DalObscuraConnectorOptions options() {
        return options;
    }

    @Override
    public String[] preferredLocations() {
        return plannedPartition.locations().toArray(new String[0]);
    }
}
