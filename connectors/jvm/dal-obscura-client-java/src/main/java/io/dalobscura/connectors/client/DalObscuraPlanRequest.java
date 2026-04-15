package io.dalobscura.connectors.client;

import java.util.List;
import java.util.Optional;

public final class DalObscuraPlanRequest {
    private final String catalog;
    private final String target;
    private final List<String> columns;
    private final Optional<String> rowFilter;

    public DalObscuraPlanRequest(
            String catalog,
            String target,
            List<String> columns,
            Optional<String> rowFilter) {
        this.catalog = catalog;
        this.target = target;
        this.columns = columns;
        this.rowFilter = rowFilter;
    }

    public String catalog() {
        return catalog;
    }

    public String target() {
        return target;
    }

    public List<String> columns() {
        return columns;
    }

    public Optional<String> rowFilter() {
        return rowFilter;
    }
}
