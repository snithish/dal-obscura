package io.dalobscura.connectors.spark.v3;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

class ArrowToSparkSchemaMapperTest {
    private final ArrowToSparkSchemaMapper mapper = new ArrowToSparkSchemaMapper();

    @Test
    void mapsPrimitiveAndNestedArrowFields() {
        Schema schema =
                new Schema(
                        List.of(
                                new Field(
                                        "id",
                                        FieldType.nullable(new ArrowType.Int(64, true)),
                                        null),
                                new Field(
                                        "email",
                                        FieldType.nullable(new ArrowType.Utf8()),
                                        null),
                                new Field(
                                        "scores",
                                        FieldType.nullable(new ArrowType.List()),
                                        List.of(
                                                new Field(
                                                        "element",
                                                        FieldType.nullable(
                                                                new ArrowType.Int(32, true)),
                                                        null))),
                                new Field(
                                        "profile",
                                        FieldType.nullable(new ArrowType.Struct()),
                                        List.of(
                                                new Field(
                                                        "active",
                                                        FieldType.nullable(new ArrowType.Bool()),
                                                        null),
                                                new Field(
                                                        "created_at",
                                                        FieldType.nullable(
                                                                new ArrowType.Timestamp(
                                                                        TimeUnit.MICROSECOND,
                                                                        null)),
                                                        null)))));

        StructType result = mapper.toStructType(schema);

        assertEquals(DataTypes.LongType, result.fields()[0].dataType());
        assertEquals(DataTypes.StringType, result.fields()[1].dataType());
        assertEquals(
                DataTypes.createArrayType(DataTypes.IntegerType, true),
                result.fields()[2].dataType());

        StructType profileType = (StructType) result.fields()[3].dataType();
        StructField activeField = profileType.fields()[0];
        StructField createdAtField = profileType.fields()[1];
        assertEquals(DataTypes.BooleanType, activeField.dataType());
        assertEquals(DataTypes.TimestampType, createdAtField.dataType());
    }

    @Test
    void mapsLargeUtf8ToSparkString() {
        Schema schema =
                new Schema(
                        List.of(
                                new Field(
                                        "email",
                                        FieldType.nullable(new ArrowType.LargeUtf8()),
                                        null)));

        StructType result = mapper.toStructType(schema);

        assertEquals(DataTypes.StringType, result.fields()[0].dataType());
    }

    @Test
    void mapsArrowMapsToSparkMaps() {
        Schema schema =
                new Schema(
                        List.of(
                                new Field(
                                        "attributes",
                                        FieldType.nullable(new ArrowType.Map(false)),
                                        List.of(
                                                new Field(
                                                        "entries",
                                                        FieldType.notNullable(
                                                                new ArrowType.Struct()),
                                                        List.of(
                                                                new Field(
                                                                        "key",
                                                                        FieldType.notNullable(
                                                                                new ArrowType
                                                                                        .Utf8()),
                                                                        null),
                                                                new Field(
                                                                        "value",
                                                                        FieldType.nullable(
                                                                                new ArrowType
                                                                                        .Utf8()),
                                                                        null)))))));

        StructType result = mapper.toStructType(schema);

        assertEquals(
                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true),
                result.fields()[0].dataType());
    }
}
