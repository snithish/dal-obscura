package io.dalobscura.connectors.spark.v3;

import io.dalobscura.connectors.client.DalObscuraPlanRequest;
import io.dalobscura.connectors.client.DalObscuraPlannedRead;
import io.dalobscura.connectors.client.DalObscuraReadClient;
import io.dalobscura.connectors.client.DalObscuraReadClientFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public final class DalObscuraScanBuilder
        implements ScanBuilder, SupportsPushDownRequiredColumns, SupportsPushDownFilters {
    private final DalObscuraConnectorOptions options;
    private final DalObscuraReadClientFactory clientFactory;
    private final SparkFilterSqlTranslator filterTranslator = new SparkFilterSqlTranslator();
    private final StructType fullSchema;
    private StructType requiredSchema;
    private SparkFilterTranslation translation =
            new SparkFilterTranslation(Optional.empty(), new Filter[0]);

    public DalObscuraScanBuilder(
            DalObscuraConnectorOptions options,
            DalObscuraReadClientFactory clientFactory,
            StructType fullSchema) {
        this.options = options;
        this.clientFactory = clientFactory;
        this.fullSchema = fullSchema;
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
        List<String> columns = plannedColumns(requiredSchema, fullSchema);
        try (DalObscuraReadClient client = clientFactory.create()) {
            DalObscuraPlannedRead plannedRead =
                    client.plan(
                            new DalObscuraPlanRequest(
                                    options.catalog(),
                                    options.target(),
                                    columns,
                                    translation.pushedSql()),
                            options.auth());
            return new DalObscuraBatch(options, clientFactory, requiredSchema, plannedRead);
        }
    }

    private static List<String> plannedColumns(
            StructType requiredSchema, StructType fullSchema) {
        List<String> projected = new ArrayList<>();
        for (StructField field : requiredSchema.fields()) {
            StructField fullField = fullSchema.apply(field.name());
            collectProjectedColumns(field, fullField, field.name(), projected);
        }
        if (!projected.isEmpty()) {
            return projected;
        }
        if (fullSchema.fields().length == 0) {
            throw new IllegalStateException("dal-obscura schema must expose at least one column");
        }
        return List.of(fullSchema.fields()[0].name());
    }

    private static void collectProjectedColumns(
            StructField requiredField,
            StructField fullField,
            String path,
            List<String> projected) {
        DataType requiredType = requiredField.dataType();
        DataType fullType = fullField.dataType();
        if (requiredType instanceof StructType && fullType instanceof StructType) {
            projected.add(path);
            return;
        }
        projected.add(path);
    }
}
