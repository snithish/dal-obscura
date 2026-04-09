package io.dalobscura.connectors.spark.v3;

import io.dalobscura.connectors.client.DalObscuraPlanRequest;
import io.dalobscura.connectors.client.DalObscuraPlannedRead;
import io.dalobscura.connectors.client.DalObscuraReadClient;
import io.dalobscura.connectors.client.DalObscuraReadClientFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

public final class DalObscuraScanBuilder
        implements ScanBuilder, SupportsPushDownRequiredColumns, SupportsPushDownFilters {
    private final DalObscuraConnectorOptions options;
    private final DalObscuraReadClientFactory clientFactory;
    private final SparkFilterSqlTranslator filterTranslator = new SparkFilterSqlTranslator();
    private StructType requiredSchema;
    private SparkFilterTranslation translation =
            new SparkFilterTranslation(Optional.empty(), new Filter[0]);

    public DalObscuraScanBuilder(
            DalObscuraConnectorOptions options,
            DalObscuraReadClientFactory clientFactory,
            StructType fullSchema) {
        this.options = options;
        this.clientFactory = clientFactory;
        this.requiredSchema = fullSchema;
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {
        this.requiredSchema = requiredSchema;
    }

    @Override
    public Filter[] pushFilters(Filter[] filters) {
        translation = filterTranslator.translate(filters);
        return translation.residualFilters();
    }

    @Override
    public Filter[] pushedFilters() {
        return new Filter[0];
    }

    @Override
    public Scan build() {
        List<String> columns = Arrays.asList(requiredSchema.fieldNames());
        try (DalObscuraReadClient client = clientFactory.create()) {
            DalObscuraPlannedRead plannedRead =
                    client.plan(
                            new DalObscuraPlanRequest(
                                    options.catalog(),
                                    options.target(),
                                    columns,
                                    translation.pushedSql()),
                            options.authToken());
            return new DalObscuraBatch(options, clientFactory, requiredSchema, plannedRead);
        }
    }
}
