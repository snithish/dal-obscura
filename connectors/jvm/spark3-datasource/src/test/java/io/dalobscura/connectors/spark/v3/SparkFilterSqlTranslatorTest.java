package io.dalobscura.connectors.spark.v3;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.Or;
import org.junit.jupiter.api.Test;

class SparkFilterSqlTranslatorTest {
    private final SparkFilterSqlTranslator translator = new SparkFilterSqlTranslator();

    @Test
    void translatesSupportedConjunctiveFilters() {
        SparkFilterTranslation translation =
                translator.translate(
                        new Filter[] {
                            new EqualTo("region", "us"),
                            new GreaterThan("id", 10)
                        });

        assertEquals("region = 'us' AND id > 10", translation.pushedSql().orElseThrow());
        assertArrayEquals(new Filter[0], translation.residualFilters());
    }

    @Test
    void keepsUnsupportedLeafFiltersResidual() {
        Filter unsupported = new Not(new EqualTo("region", "us"));

        SparkFilterTranslation translation = translator.translate(new Filter[] {unsupported});

        assertTrue(translation.pushedSql().isEmpty());
        assertArrayEquals(new Filter[] {unsupported}, translation.residualFilters());
    }

    @Test
    void preservesWholeOrExpressionWhenOneSideIsUnsupported() {
        Filter mixed =
                new Or(
                        new EqualTo("region", "us"),
                        new Not(new EqualTo("region", "eu")));

        SparkFilterTranslation translation = translator.translate(new Filter[] {mixed});

        assertTrue(translation.pushedSql().isEmpty());
        assertArrayEquals(new Filter[] {mixed}, translation.residualFilters());
    }

    @Test
    void partiallyPushesSupportedChildrenOfAnd() {
        Filter mixedAnd =
                new And(
                        new EqualTo("region", "us"),
                        new Not(new EqualTo("region", "eu")));

        SparkFilterTranslation translation = translator.translate(new Filter[] {mixedAnd});

        assertEquals("region = 'us'", translation.pushedSql().orElseThrow());
        assertArrayEquals(new Filter[] {new Not(new EqualTo("region", "eu"))}, translation.residualFilters());
    }
}
