package io.dalobscura.connectors.spark.v3;

import io.dalobscura.connectors.client.DalObscuraReadClient;
import io.dalobscura.connectors.client.DalObscuraReadClientFactory;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public final class DalObscuraTable implements Table, SupportsRead {
    private final DalObscuraConnectorOptions options;
    private final DalObscuraReadClientFactory clientFactory;
    private final ArrowToSparkSchemaMapper schemaMapper = new ArrowToSparkSchemaMapper();
    private StructType schema;

    public DalObscuraTable(
            DalObscuraConnectorOptions options, DalObscuraReadClientFactory clientFactory) {
        this.options = options;
        this.clientFactory = clientFactory;
    }

    @Override
    public String name() {
        return options.catalog() + "." + options.target();
    }

    @Override
    public StructType schema() {
        if (schema == null) {
            try (DalObscuraReadClient client = clientFactory.create()) {
                schema =
                        schemaMapper.toStructType(
                                client.fetchSchema(
                                        options.catalog(), options.target(), options.auth()));
            }
        }
        return schema;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap optionsMap) {
        return new DalObscuraScanBuilder(options, clientFactory, schema());
    }

    @Override
    public Set<TableCapability> capabilities() {
        return Set.of(TableCapability.BATCH_READ);
    }
}
