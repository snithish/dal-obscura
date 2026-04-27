package io.dalobscura.connectors.client;

public interface DalObscuraReadClient extends AutoCloseable {
    org.apache.arrow.vector.types.pojo.Schema fetchSchema(
            String catalog, String target, DalObscuraAuth auth);

    DalObscuraPlannedRead plan(DalObscuraPlanRequest request, DalObscuraAuth auth);

    DalObscuraTicketStream openStream(DalObscuraPlannedPartition partition, DalObscuraAuth auth);

    @Override
    void close();
}
