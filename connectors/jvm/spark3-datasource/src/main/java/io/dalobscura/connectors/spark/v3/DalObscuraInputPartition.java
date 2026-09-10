package io.dalobscura.connectors.spark.v3;

import io.dalobscura.connectors.client.DalObscuraPlannedPartition;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.types.StructType;

/** Spark-serialized work unit. It deliberately excludes driver credentials. */
public final class DalObscuraInputPartition implements InputPartition {
    private final DalObscuraPlannedPartition plannedPartition;
    private final DalObscuraExecutorAuthProvider executorAuthProvider;
    private final StructType requiredSchema;

    public DalObscuraInputPartition(
            DalObscuraPlannedPartition plannedPartition,
            DalObscuraExecutorAuthProvider executorAuthProvider,
            StructType requiredSchema) {
        this.plannedPartition = plannedPartition;
        this.executorAuthProvider = executorAuthProvider;
        this.requiredSchema = requiredSchema;
    }

    public DalObscuraPlannedPartition plannedPartition() {
        return plannedPartition;
    }

    public DalObscuraExecutorAuthProvider executorAuthProvider() {
        return executorAuthProvider;
    }

    public StructType requiredSchema() {
        return requiredSchema;
    }

    @Override
    public String[] preferredLocations() {
        return plannedPartition.locations().toArray(new String[0]);
    }
}
