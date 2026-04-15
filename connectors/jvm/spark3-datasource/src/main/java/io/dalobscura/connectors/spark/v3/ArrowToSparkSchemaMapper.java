package io.dalobscura.connectors.spark.v3;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public final class ArrowToSparkSchemaMapper {
    public StructType toStructType(Schema schema) {
        List<StructField> fields = new ArrayList<>();
        for (Field field : schema.getFields()) {
            fields.add(
                    new StructField(
                            field.getName(),
                            toSparkType(field),
                            field.isNullable(),
                            Metadata.empty()));
        }
        return DataTypes.createStructType(fields);
    }

    private DataType toSparkType(Field field) {
        ArrowType type = field.getType();
        if (type instanceof ArrowType.Int) {
            ArrowType.Int intType = (ArrowType.Int) type;
            if (intType.getBitWidth() == 64) {
                return DataTypes.LongType;
            }
            if (intType.getBitWidth() == 32) {
                return DataTypes.IntegerType;
            }
        }
        if (type instanceof ArrowType.Bool) {
            return DataTypes.BooleanType;
        }
        if (type instanceof ArrowType.Utf8 || type instanceof ArrowType.LargeUtf8) {
            return DataTypes.StringType;
        }
        if (type instanceof ArrowType.Binary || type instanceof ArrowType.LargeBinary) {
            return DataTypes.BinaryType;
        }
        if (type instanceof ArrowType.Date) {
            return DataTypes.DateType;
        }
        if (type instanceof ArrowType.Timestamp) {
            return DataTypes.TimestampType;
        }
        if (type instanceof ArrowType.Decimal) {
            ArrowType.Decimal decimal = (ArrowType.Decimal) type;
            return DataTypes.createDecimalType(decimal.getPrecision(), decimal.getScale());
        }
        if (type instanceof ArrowType.FloatingPoint) {
            ArrowType.FloatingPoint floatingPoint = (ArrowType.FloatingPoint) type;
            switch (floatingPoint.getPrecision()) {
                case SINGLE:
                    return DataTypes.FloatType;
                case DOUBLE:
                    return DataTypes.DoubleType;
                default:
                    throw new IllegalArgumentException(
                            "Unsupported Arrow floating precision: "
                                    + floatingPoint.getPrecision());
            }
        }
        if (type instanceof ArrowType.List || type instanceof ArrowType.LargeList) {
            Field elementField = field.getChildren().get(0);
            return DataTypes.createArrayType(toSparkType(elementField), elementField.isNullable());
        }
        if (type instanceof ArrowType.Map) {
            Field entriesField = field.getChildren().get(0);
            Field keyField = entriesField.getChildren().get(0);
            Field valueField = entriesField.getChildren().get(1);
            return DataTypes.createMapType(
                    toSparkType(keyField), toSparkType(valueField), valueField.isNullable());
        }
        if (type instanceof ArrowType.Struct) {
            return toStructType(new Schema(field.getChildren()));
        }
        throw new IllegalArgumentException(
                "Unsupported Arrow field type for Spark mapping: " + type);
    }
}
