PARSER_MULTIPLE_STATEMENT_ROW_FILTERS = [
    "region = 'us'; DROP TABLE input",
    "region = 'us'; SELECT 1",
    "region = 'us'; COPY (SELECT 1) TO '/tmp/leak.csv'",
]

PARSER_NON_FILTER_STATEMENT_ROW_FILTERS = [
    "COPY input TO '/tmp/leak.csv'",
    "ATTACH 'tenant.duckdb' AS tenant",
    "INSTALL httpfs",
    "LOAD httpfs",
    "CREATE TABLE stolen AS SELECT 1",
    "DROP TABLE input",
]

PARSER_UNSAFE_EXPRESSION_ROW_FILTERS = [
    "EXISTS(SELECT 1)",
    "id IN (SELECT 1)",
    "read_csv('/etc/passwd')",
    "region = (SELECT 'us')",
]

FLIGHT_UNSAFE_ROW_FILTER_SMOKE_CASES = [
    PARSER_MULTIPLE_STATEMENT_ROW_FILTERS[0],
    PARSER_NON_FILTER_STATEMENT_ROW_FILTERS[0],
    PARSER_UNSAFE_EXPRESSION_ROW_FILTERS[0],
]
