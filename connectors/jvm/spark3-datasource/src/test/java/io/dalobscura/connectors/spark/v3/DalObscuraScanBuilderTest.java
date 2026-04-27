package io.dalobscura.connectors.spark.v3;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.dalobscura.connectors.client.DalObscuraAuth;
import io.dalobscura.connectors.client.DalObscuraPlanRequest;
import io.dalobscura.connectors.client.DalObscuraPlannedPartition;
import io.dalobscura.connectors.client.DalObscuraPlannedRead;
import io.dalobscura.connectors.client.DalObscuraReadClient;
import io.dalobscura.connectors.client.DalObscuraTicketStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;

class DalObscuraScanBuilderTest {
    @Test
    void plansWildcardSchemaThenPlansProjectedTicketRead() {
        RecordingClient client = new RecordingClient();
        DalObscuraAuth auth = new DalObscuraAuth(Map.of("x-api-key", "secret-1"));
        DalObscuraTable table =
                new DalObscuraTable(
                        new DalObscuraConnectorOptions(
                                "grpc+tcp://localhost:8815", "analytics", "default.users", auth),
                        () -> client);

        StructType fullSchema = table.schema();
        DalObscuraScanBuilder builder =
                (DalObscuraScanBuilder) table.newScanBuilder(new CaseInsensitiveStringMap(Map.of()));
        builder.pruneColumns(new StructType().add("id", "long"));
        builder.pushFilters(new org.apache.spark.sql.sources.Filter[] {new EqualTo("region", "us")});

        InputPartition[] partitions = builder.build().toBatch().planInputPartitions();

        assertEquals(1, fullSchema.fields().length);
        assertEquals("id", fullSchema.fields()[0].name());
        assertEquals(1, client.schemaRequestCount());
        assertEquals("secret-1", client.schemaAuths().get(0).header("x-api-key"));
        assertEquals(List.of("id"), client.planRequests().get(0).columns());
        assertEquals(Optional.of("region = 'us'"), client.planRequests().get(0).rowFilter());
        assertEquals("secret-1", client.planAuths().get(0).header("x-api-key"));
        assertEquals(2, partitions.length);
    }

    @Test
    void createsSerializableInputPartitions() throws Exception {
        RecordingClient client = new RecordingClient();
        DalObscuraTable table =
                new DalObscuraTable(
                        new DalObscuraConnectorOptions(
                                "grpc+tcp://localhost:8815",
                                "analytics",
                                "default.users",
                                DalObscuraAuth.bearerToken("token-123")),
                        () -> client);

        DalObscuraScanBuilder builder =
                (DalObscuraScanBuilder) table.newScanBuilder(new CaseInsensitiveStringMap(Map.of()));
        InputPartition[] partitions = builder.build().toBatch().planInputPartitions();

        try (ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                ObjectOutputStream output = new ObjectOutputStream(buffer)) {
            output.writeObject(partitions[0]);
        }
    }

    @Test
    void plansNestedProjectedColumnsAsTopLevelStructs() {
        RecordingClient client = new RecordingClient();
        StructType fullSchema =
                new StructType()
                        .add(
                                "user",
                                new StructType()
                                        .add(
                                                "address",
                                                new StructType().add("zip", "string").add("city", "string"))
                                        .add("email", "string"));
        DalObscuraScanBuilder builder =
                new DalObscuraScanBuilder(
                        new DalObscuraConnectorOptions(
                                "grpc+tcp://localhost:8815",
                                "analytics",
                                "default.users",
                                DalObscuraAuth.bearerToken("token-123")),
                        () -> client,
                        fullSchema);

        builder.pruneColumns(
                new StructType()
                        .add(
                                "user",
                                new StructType().add("address", new StructType().add("zip", "string"))));

        builder.build();

        assertEquals(List.of("user"), client.planRequests().get(0).columns());
    }

    @Test
    void fallsBackToTheFirstVisibleColumnWhenSparkPrunesAllColumns() {
        RecordingClient client = new RecordingClient();
        StructType fullSchema = new StructType().add("id", "long").add("region", "string");
        DalObscuraScanBuilder builder =
                new DalObscuraScanBuilder(
                        new DalObscuraConnectorOptions(
                                "grpc+tcp://localhost:8815",
                                "analytics",
                                "default.users",
                                DalObscuraAuth.bearerToken("token-123")),
                        () -> client,
                        fullSchema);

        builder.pruneColumns(new StructType());

        builder.build();

        assertEquals(List.of("id"), client.planRequests().get(0).columns());
    }

    private static final class RecordingClient implements DalObscuraReadClient {
        private final ArrayList<DalObscuraPlanRequest> planRequests = new ArrayList<>();
        private final ArrayList<DalObscuraAuth> planAuths = new ArrayList<>();
        private final ArrayList<DalObscuraAuth> schemaAuths = new ArrayList<>();
        private int schemaRequestCount;

        @Override
        public Schema fetchSchema(String catalog, String target, DalObscuraAuth auth) {
            schemaRequestCount += 1;
            schemaAuths.add(auth);
            return new Schema(
                    List.of(
                            new Field(
                                    "id",
                                    FieldType.nullable(new ArrowType.Int(64, true)),
                                    null)));
        }

        @Override
        public DalObscuraPlannedRead plan(DalObscuraPlanRequest request, DalObscuraAuth auth) {
            planRequests.add(request);
            planAuths.add(auth);
            Schema schema =
                    new Schema(
                            List.of(
                                    new Field(
                                            "id",
                                            FieldType.nullable(new ArrowType.Int(64, true)),
                                            null)));
            return new DalObscuraPlannedRead(
                    schema,
                    List.of(
                            new DalObscuraPlannedPartition("ticket-a"),
                            new DalObscuraPlannedPartition("ticket-b")));
        }

        @Override
        public DalObscuraTicketStream openStream(
                DalObscuraPlannedPartition partition, DalObscuraAuth auth) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {}

        List<DalObscuraPlanRequest> planRequests() {
            return planRequests;
        }

        List<DalObscuraAuth> planAuths() {
            return planAuths;
        }

        List<DalObscuraAuth> schemaAuths() {
            return schemaAuths;
        }

        int schemaRequestCount() {
            return schemaRequestCount;
        }
    }
}
