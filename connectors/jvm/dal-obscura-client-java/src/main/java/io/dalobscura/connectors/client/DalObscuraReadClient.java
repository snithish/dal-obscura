package io.dalobscura.connectors.client;

public interface DalObscuraReadClient extends AutoCloseable {
    org.apache.arrow.vector.types.pojo.Schema fetchSchema(
            String catalog, String target, String authToken);

    DalObscuraPlannedRead plan(DalObscuraPlanRequest request, String authToken);

    DalObscuraTicketStream openStream(DalObscuraPlannedPartition partition, String authToken);

    @Override
    void close();
}
