package io.dalobscura.connectors.spark.v3;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Timestamp;
import java.util.stream.Stream;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.Or;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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

        assertEquals("\"region\" = 'us' AND \"id\" > 10", translation.pushedSql().orElseThrow());
        assertArrayEquals(
                new Filter[] {new EqualTo("region", "us"), new GreaterThan("id", 10)},
                translation.pushedFilters());
        assertArrayEquals(new Filter[0], translation.residualFilters());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("residualFilters")
    void preservesUnsupportedFiltersForSpark(String scenario, Filter unsupported) {
        SparkFilterTranslation translation = translator.translate(new Filter[] {unsupported});

        assertTrue(translation.pushedSql().isEmpty());
        assertArrayEquals(new Filter[0], translation.pushedFilters());
        assertArrayEquals(new Filter[] {unsupported}, translation.residualFilters());
    }

    private static Stream<Arguments> residualFilters() {
        return Stream.of(
                Arguments.of("unsupported NOT", new Not(new EqualTo("region", "us"))),
                Arguments.of("OR with unsupported branch", new Or(
                        new EqualTo("region", "us"), new Not(new EqualTo("region", "eu")))),
                Arguments.of("empty IN", new In("region", new Object[0])),
                Arguments.of("binary literal", new EqualTo("payload", new byte[] {1, 2, 3})));
    }

    @Test
    void partiallyPushesSupportedChildrenOfAnd() {
        Filter mixedAnd =
                new And(
                        new EqualTo("region", "us"),
                        new Not(new EqualTo("region", "eu")));

        SparkFilterTranslation translation = translator.translate(new Filter[] {mixedAnd});

        assertEquals("\"region\" = 'us'", translation.pushedSql().orElseThrow());
        assertArrayEquals(new Filter[] {new EqualTo("region", "us")}, translation.pushedFilters());
        assertArrayEquals(new Filter[] {new Not(new EqualTo("region", "eu"))}, translation.residualFilters());
    }

    @Test
    void preservesOrGroupingInsideConjunctions() {
        Filter grouped =
                new And(
                        new EqualTo("id", 5),
                        new Or(
                                new EqualTo("region", "us"),
                                new EqualTo("region", "eu")));

        SparkFilterTranslation translation = translator.translate(new Filter[] {grouped});

        assertEquals(
                "\"id\" = 5 AND ((\"region\" = 'us') OR (\"region\" = 'eu'))",
                translation.pushedSql().orElseThrow());
        assertArrayEquals(new Filter[] {grouped}, translation.pushedFilters());
        assertArrayEquals(new Filter[0], translation.residualFilters());
    }

    @Test
    void preservesOrGroupingAcrossTopLevelConjuncts() {
        Filter disjunction =
                new Or(
                        new EqualTo("region", "us"),
                        new EqualTo("region", "eu"));

        SparkFilterTranslation translation =
                translator.translate(new Filter[] {disjunction, new EqualTo("active", true)});

        assertEquals(
                "((\"region\" = 'us') OR (\"region\" = 'eu')) AND \"active\" = true",
                translation.pushedSql().orElseThrow());
        assertArrayEquals(new Filter[] {disjunction, new EqualTo("active", true)}, translation.pushedFilters());
        assertArrayEquals(new Filter[0], translation.residualFilters());
    }

    @Test
    void rendersTimestampLiteralsAsSqlTimestampLiterals() {
        Timestamp timestamp = Timestamp.valueOf("2024-01-01 12:16:00");

        SparkFilterTranslation translation =
                translator.translate(new Filter[] {new EqualTo("created_at", timestamp)});

        assertEquals(
                "\"created_at\" = TIMESTAMP '2024-01-01 12:16:00.0'",
                translation.pushedSql().orElseThrow());
        assertArrayEquals(new Filter[0], translation.residualFilters());
    }

    @Test
    void quotesEachAttributePathSegment() {
        SparkFilterTranslation translation =
                translator.translate(new Filter[] {new EqualTo("account.manager.region", "amer")});

        assertEquals(
                "\"account\".\"manager\".\"region\" = 'amer'",
                translation.pushedSql().orElseThrow());
        assertArrayEquals(
                new Filter[] {new EqualTo("account.manager.region", "amer")},
                translation.pushedFilters());
        assertArrayEquals(new Filter[0], translation.residualFilters());
    }
}
