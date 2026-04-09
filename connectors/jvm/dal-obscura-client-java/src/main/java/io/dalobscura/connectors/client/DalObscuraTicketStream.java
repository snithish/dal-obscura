package io.dalobscura.connectors.client;

import org.apache.arrow.vector.VectorSchemaRoot;

public interface DalObscuraTicketStream extends AutoCloseable {
    boolean next();

    VectorSchemaRoot root();

    @Override
    void close();
}
