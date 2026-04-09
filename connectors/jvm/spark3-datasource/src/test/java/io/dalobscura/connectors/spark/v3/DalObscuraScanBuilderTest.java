package io.dalobscura.connectors.spark.v3;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
        DalObscuraTable table =
                new DalObscuraTable(
                        new DalObscuraConnectorOptions(
                                "grpc+tcp://localhost:8815",
                                "analytics",
                                "default.users",
                                "token-123"),
                        () -> client);

        StructType fullSchema = table.schema();
        DalObscuraScanBuilder builder =
                (DalObscuraScanBuilder) table.newScanBuilder(new CaseInsensitiveStringMap(Map.of()));
        builder.pruneColumns(new StructType().add("id", "long"));
        builder.pushFilters(new org.apache.spark.sql.sources.Filter[] {new EqualTo("region", "us")});

        InputPartition[] partitions = builder.build().toBatch().planInputPartitions();

        assertEquals(1, fullSchema.fields().length);
        assertEquals("id", fullSchema.fields()[0].name());
        assertEquals(List.of("*"), client.planRequests().get(0).columns());
        assertEquals(Optional.empty(), client.planRequests().get(0).rowFilter());
        assertEquals(List.of("id"), client.planRequests().get(1).columns());
        assertEquals(Optional.of("region = 'us'"), client.planRequests().get(1).rowFilter());
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
                                "token-123"),
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
    void plansNestedProjectedColumnsAsDottedPaths() {
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
                                "token-123"),
                        () -> client,
                        fullSchema);

        builder.pruneColumns(
                new StructType()
                        .add(
                                "user",
                                new StructType().add("address", new StructType().add("zip", "string"))));

        builder.build();

        assertEquals(List.of("user.address.zip"), client.planRequests().get(0).columns());
    }

    private static final class RecordingClient implements DalObscuraReadClient {
        private final ArrayList<DalObscuraPlanRequest> planRequests = new ArrayList<>();

        @Override
        public DalObscuraPlannedRead plan(DalObscuraPlanRequest request, String authToken) {
            planRequests.add(request);
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
                DalObscuraPlannedPartition partition, String authToken) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {}

        List<DalObscuraPlanRequest> planRequests() {
            return planRequests;
        }
    }
}
