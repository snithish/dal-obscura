package io.dalobscura.connectors.spark.v3;

import java.util.Optional;
import org.apache.spark.sql.sources.Filter;

public final class SparkFilterTranslation {
    private final Optional<String> pushedSql;
    private final Filter[] pushedFilters;
    private final Filter[] residualFilters;

    public SparkFilterTranslation(Optional<String> pushedSql, Filter[] pushedFilters, Filter[] residualFilters) {
        this.pushedSql = pushedSql;
        this.pushedFilters = pushedFilters;
        this.residualFilters = residualFilters;
    }

    public Optional<String> pushedSql() {
        return pushedSql;
    }

    public Filter[] pushedFilters() {
        return pushedFilters;
    }

    public Filter[] residualFilters() {
        return residualFilters;
    }
}
