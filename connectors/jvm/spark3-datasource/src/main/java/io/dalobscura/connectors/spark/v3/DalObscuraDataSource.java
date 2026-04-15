package io.dalobscura.connectors.spark.v3;

import io.dalobscura.connectors.client.DalObscuraReadClientFactory;
import io.dalobscura.connectors.client.FlightDalObscuraReadClient;
import java.util.Map;
import org.apache.spark.sql.connector.catalog.SessionConfigSupport;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public final class DalObscuraDataSource
        implements TableProvider, SessionConfigSupport, DataSourceRegister {
    private final DalObscuraOptionsResolver resolver = new DalObscuraOptionsResolver();

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        return tableFor(options).schema();
    }

    @Override
    public Table getTable(
            StructType schema, Transform[] partitioning, Map<String, String> properties) {
        return tableFor(new CaseInsensitiveStringMap(properties));
    }

    @Override
    public String shortName() {
        return "dal_obscura";
    }

    @Override
    public String keyPrefix() {
        return "dal_obscura";
    }

    private DalObscuraTable tableFor(CaseInsensitiveStringMap options) {
        DalObscuraConnectorOptions resolved = resolver.resolve(options);
        DalObscuraReadClientFactory clientFactory =
                () -> new FlightDalObscuraReadClient(resolved.uri());
        return new DalObscuraTable(resolved, clientFactory);
    }
}
