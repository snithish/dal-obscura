package io.dalobscura.connectors.spark.v3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.dalobscura.connectors.client.DalObscuraPlanRequest;
import io.dalobscura.connectors.client.DalObscuraPlannedPartition;
import io.dalobscura.connectors.client.DalObscuraPlannedRead;
import io.dalobscura.connectors.client.DalObscuraReadClient;
import io.dalobscura.connectors.client.DalObscuraTicketStream;
import java.util.List;
import java.util.Optional;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.Test;

class DalObscuraPartitionReaderTest {
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

            try (DalObscuraPartitionReader reader =
                    new DalObscuraPartitionReader(
                            new FakeClient(root),
                            new DalObscuraInputPartition(
                                    new DalObscuraPlannedPartition("ticket-a"),
                                    new DalObscuraConnectorOptions(
                                            "grpc+tcp://localhost:8815",
                                            "analytics",
                                            "default.users",
                                            "token-123")))) {
                assertTrue(reader.next());
                ColumnarBatch batch = reader.get();
                assertEquals(2, batch.numRows());
                assertEquals(1L, batch.column(0).getLong(0));
                assertEquals(2L, batch.column(0).getLong(1));
            }
        }
    }

    private static final class FakeClient implements DalObscuraReadClient {
        private final VectorSchemaRoot root;

        private FakeClient(VectorSchemaRoot root) {
            this.root = root;
        }

        @Override
        public DalObscuraPlannedRead plan(DalObscuraPlanRequest request, String authToken) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DalObscuraTicketStream openStream(
                DalObscuraPlannedPartition partition, String authToken) {
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
                public void close() {}
            };
        }

        @Override
        public void close() {}
    }
}
