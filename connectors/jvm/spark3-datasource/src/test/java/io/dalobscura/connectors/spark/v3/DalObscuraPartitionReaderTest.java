package io.dalobscura.connectors.spark.v3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.dalobscura.connectors.client.DalObscuraAuth;
import io.dalobscura.connectors.client.DalObscuraPlanRequest;
import io.dalobscura.connectors.client.DalObscuraPlannedPartition;
import io.dalobscura.connectors.client.DalObscuraPlannedRead;
import io.dalobscura.connectors.client.DalObscuraReadClient;
import io.dalobscura.connectors.client.DalObscuraTicketStream;
import java.util.List;
import java.util.Optional;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

class DalObscuraPartitionReaderTest {
    private static DalObscuraExecutorAuthProvider executorAuth() {
        System.setProperty("dal.obscura.test.token", "executor-token");
        return DalObscuraExecutorAuthProvider.fromSystemProperty("dal.obscura.test.token");
    }

    @Test
    void returnsSparkColumnarBatchesFromArrowRoots() throws Exception {
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            BigIntVector id = new BigIntVector("id", allocator);
            id.allocateNew(2);
            id.set(0, 1L);
            id.set(1, 2L);
            id.setValueCount(2);

            VectorSchemaRoot root =
                    new VectorSchemaRoot(List.of(id.getField()), List.of(id), 2);
            FakeClient client = new FakeClient(root);

            try (DalObscuraPartitionReader reader =
                    new DalObscuraPartitionReader(
                            client,
                            new DalObscuraInputPartition(
                                    new DalObscuraPlannedPartition("ticket-a"),
                                    executorAuth(),
                                    new org.apache.spark.sql.types.StructType().add("id", "long")))) {
                assertTrue(reader.next());
                ColumnarBatch batch = reader.get();
                assertEquals(2, batch.numRows());
                assertEquals(1L, batch.column(0).getLong(0));
                assertEquals(2L, batch.column(0).getLong(1));
                assertEquals("Bearer executor-token", client.lastStreamAuth().header("authorization"));
            }
        }
    }

    @Test
    void reshapesStructVectorsToMatchSparkPrunedNestedSchema() throws Exception {
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            StructVector account = StructVector.empty("account", allocator);
            VarCharVector status =
                    account.addOrGet("status", FieldType.nullable(new ArrowType.Utf8()), VarCharVector.class);
            VarCharVector tier =
                    account.addOrGet("tier", FieldType.nullable(new ArrowType.Utf8()), VarCharVector.class);
            StructVector manager =
                    account.addOrGet(
                            "manager",
                            FieldType.nullable(ArrowType.Struct.INSTANCE),
                            StructVector.class);
            VarCharVector region =
                    manager.addOrGet("region", FieldType.nullable(new ArrowType.Utf8()), VarCharVector.class);

            status.allocateNew();
            tier.allocateNew();
            region.allocateNew();

            account.setIndexDefined(0);
            manager.setIndexDefined(0);
            status.set(0, "enabled".getBytes());
            tier.set(0, "gold".getBytes());
            region.set(0, "amer".getBytes());

            status.setValueCount(1);
            tier.setValueCount(1);
            region.setValueCount(1);
            manager.setValueCount(1);
            account.setValueCount(1);

            VectorSchemaRoot root =
                    new VectorSchemaRoot(List.of(account.getField()), List.of(account), 1);
            org.apache.spark.sql.types.StructType requiredSchema =
                    new org.apache.spark.sql.types.StructType()
                            .add(
                                    "account",
                                    new org.apache.spark.sql.types.StructType()
                                            .add(
                                                    "manager",
                                                    new org.apache.spark.sql.types.StructType()
                                                            .add("region", "string")));

            try (DalObscuraPartitionReader reader =
                    new DalObscuraPartitionReader(
                            new FakeClient(root),
                            new DalObscuraInputPartition(
                                    new DalObscuraPlannedPartition("ticket-a"),
                                    executorAuth(),
                                    requiredSchema))) {
                assertTrue(reader.next());
                ColumnarBatch batch = reader.get();
                UTF8String value = batch.column(0).getStruct(0).getStruct(0, 1).getUTF8String(0);
                assertEquals("amer", value.toString());
            }
        }
    }

    @Test
    void closesClientWhenOpeningTheTicketStreamFails() {
        FakeClient client = new FakeClient(null);
        client.failOpen = true;

        assertThrows(
                IllegalStateException.class,
                () ->
                        new DalObscuraPartitionReader(
                                client,
                                new DalObscuraInputPartition(
                                        new DalObscuraPlannedPartition("ticket-a"),
                                        executorAuth(),
                                        new org.apache.spark.sql.types.StructType())));
        assertEquals(1, client.closeCount);
    }

    @Test
    void closesResourcesOnceWhenCloseIsCalledRepeatedly() {
        FakeClient client = new FakeClient(null);
        DalObscuraPartitionReader reader =
                new DalObscuraPartitionReader(
                        client,
                        new DalObscuraInputPartition(
                                new DalObscuraPlannedPartition("ticket-a"),
                                executorAuth(),
                                new org.apache.spark.sql.types.StructType()));

        reader.close();
        reader.close();

        assertEquals(1, client.closeCount);
        assertEquals(1, client.streamCloseCount);
    }

    private static final class FakeClient implements DalObscuraReadClient {
        private final VectorSchemaRoot root;
        private DalObscuraAuth lastStreamAuth;
        private boolean failOpen;
        private int closeCount;
        private int streamCloseCount;

        private FakeClient(VectorSchemaRoot root) {
            this.root = root;
        }

        @Override
        public org.apache.arrow.vector.types.pojo.Schema fetchSchema(
                String catalog, String target, DalObscuraAuth auth) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DalObscuraPlannedRead plan(DalObscuraPlanRequest request, DalObscuraAuth auth) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DalObscuraTicketStream openStream(
                DalObscuraPlannedPartition partition, DalObscuraAuth auth) {
            if (failOpen) {
                throw new IllegalStateException("stream unavailable");
            }
            lastStreamAuth = auth;
            return new DalObscuraTicketStream() {
                private boolean consumed;

                @Override
                public boolean next() {
                    boolean result = !consumed;
                    consumed = true;
                    return result;
                }

                @Override
                public VectorSchemaRoot root() {
                    return root;
                }

                @Override
                public void close() {
                    streamCloseCount++;
                }
            };
        }

        @Override
        public void close() {
            closeCount++;
        }

        DalObscuraAuth lastStreamAuth() {
            return lastStreamAuth;
        }
    }
}
