package io.dalobscura.connectors.client;

public interface DalObscuraReadClient extends AutoCloseable {
    DalObscuraPlannedRead plan(DalObscuraPlanRequest request, String authToken);

    DalObscuraTicketStream openStream(DalObscuraPlannedPartition partition, String authToken);

    @Override
    void close();
}
