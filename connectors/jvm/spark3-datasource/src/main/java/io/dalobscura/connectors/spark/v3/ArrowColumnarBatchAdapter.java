package io.dalobscura.connectors.spark.v3;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public final class ArrowColumnarBatchAdapter implements AutoCloseable {
    private final StructType requiredSchema;

    public ArrowColumnarBatchAdapter(StructType requiredSchema) {
        this.requiredSchema = requiredSchema;
    }

    public ColumnarBatch adapt(VectorSchemaRoot root) {
        Map<String, FieldVector> vectorsByName = new LinkedHashMap<>();
        List<FieldVector> fieldVectors = root.getFieldVectors();
        for (FieldVector fieldVector : fieldVectors) {
            vectorsByName.put(fieldVector.getName(), fieldVector);
        }

        ColumnVector[] vectors = new ColumnVector[requiredSchema.fields().length];
        StructField[] fields = requiredSchema.fields();
        for (int index = 0; index < fields.length; index++) {
            StructField field = fields[index];
            FieldVector fieldVector = vectorsByName.get(field.name());
            if (fieldVector == null) {
                throw new IllegalStateException("Missing Arrow vector for Spark field: " + field.name());
            }
            vectors[index] =
                    ProjectedColumnVector.project(
                            new ArrowColumnVector(fieldVector), field.dataType(), true);
        }
        return new ColumnarBatch(vectors, root.getRowCount());
    }

    @Override
    public void close() {}
}
