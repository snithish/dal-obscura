package io.dalobscura.connectors.spark.v3;

import java.util.Optional;
import org.apache.spark.sql.sources.Filter;

public final class SparkFilterTranslation {
    private final Optional<String> pushedSql;
    private final Filter[] residualFilters;

    public SparkFilterTranslation(Optional<String> pushedSql, Filter[] residualFilters) {
        this.pushedSql = pushedSql;
        this.residualFilters = residualFilters;
    }

    public Optional<String> pushedSql() {
        return pushedSql;
    }

    public Filter[] residualFilters() {
        return residualFilters;
    }
}
