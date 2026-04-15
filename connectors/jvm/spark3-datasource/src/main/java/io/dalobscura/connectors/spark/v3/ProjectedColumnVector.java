package io.dalobscura.connectors.spark.v3;

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;

final class ProjectedColumnVector extends ColumnVector {
    private final ColumnVector delegate;
    private final ColumnVector[] childVectors;
    private final boolean ownsDelegate;

    private ProjectedColumnVector(
            ColumnVector delegate, DataType dataType, ColumnVector[] childVectors, boolean ownsDelegate) {
        super(dataType);
        this.delegate = delegate;
        this.childVectors = childVectors;
        this.ownsDelegate = ownsDelegate;
    }

    static ColumnVector project(ColumnVector delegate, DataType requiredType, boolean ownsDelegate) {
        if (requiredType instanceof StructType && delegate.dataType() instanceof StructType) {
            StructType requiredStruct = (StructType) requiredType;
            StructType actualStruct = (StructType) delegate.dataType();
            ColumnVector[] children = new ColumnVector[requiredStruct.fields().length];
            StructField[] requiredFields = requiredStruct.fields();
            for (int index = 0; index < requiredFields.length; index++) {
                StructField field = requiredFields[index];
                int actualOrdinal = actualStruct.fieldIndex(field.name());
                children[index] =
                        project(delegate.getChild(actualOrdinal), field.dataType(), false);
            }
            return new ProjectedColumnVector(delegate, requiredType, children, ownsDelegate);
        }
        return new ProjectedColumnVector(delegate, requiredType, new ColumnVector[0], ownsDelegate);
    }

    @Override
    public boolean hasNull() {
        return delegate.hasNull();
    }

    @Override
    public int numNulls() {
        return delegate.numNulls();
    }

    @Override
    public boolean isNullAt(int rowId) {
        return delegate.isNullAt(rowId);
    }

    @Override
    public boolean getBoolean(int rowId) {
        return delegate.getBoolean(rowId);
    }

    @Override
    public byte getByte(int rowId) {
        return delegate.getByte(rowId);
    }

    @Override
    public short getShort(int rowId) {
        return delegate.getShort(rowId);
    }

    @Override
    public int getInt(int rowId) {
        return delegate.getInt(rowId);
    }

    @Override
    public long getLong(int rowId) {
        return delegate.getLong(rowId);
    }

    @Override
    public float getFloat(int rowId) {
        return delegate.getFloat(rowId);
    }

    @Override
    public double getDouble(int rowId) {
        return delegate.getDouble(rowId);
    }

    @Override
    public org.apache.spark.sql.types.Decimal getDecimal(int rowId, int precision, int scale) {
        return delegate.getDecimal(rowId, precision, scale);
    }

    @Override
    public org.apache.spark.unsafe.types.UTF8String getUTF8String(int rowId) {
        return delegate.getUTF8String(rowId);
    }

    @Override
    public byte[] getBinary(int rowId) {
        return delegate.getBinary(rowId);
    }

    @Override
    public ColumnarArray getArray(int rowId) {
        if (!(dataType() instanceof ArrayType)) {
            return delegate.getArray(rowId);
        }
        return delegate.getArray(rowId);
    }

    @Override
    public ColumnarMap getMap(int rowId) {
        if (!(dataType() instanceof MapType)) {
            return delegate.getMap(rowId);
        }
        return delegate.getMap(rowId);
    }

    @Override
    public ColumnVector getChild(int ordinal) {
        return childVectors[ordinal];
    }

    @Override
    public void close() {
        if (ownsDelegate) {
            delegate.close();
        }
    }
}
