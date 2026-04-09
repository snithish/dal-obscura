package io.dalobscura.connectors.spark.v3;

import java.util.List;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public final class ArrowColumnarBatchAdapter {
    public ColumnarBatch adapt(VectorSchemaRoot root) {
        List<FieldVector> fieldVectors = root.getFieldVectors();
        ColumnVector[] vectors = new ColumnVector[fieldVectors.size()];
        for (int index = 0; index < fieldVectors.size(); index++) {
            vectors[index] = new ArrowColumnVector(fieldVectors.get(index));
        }
        return new ColumnarBatch(vectors, root.getRowCount());
    }
}
