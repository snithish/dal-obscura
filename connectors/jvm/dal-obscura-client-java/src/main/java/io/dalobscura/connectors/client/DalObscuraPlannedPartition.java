package io.dalobscura.connectors.client;

import java.util.List;

public final class DalObscuraPlannedPartition {
    private final String ticket;
    private final List<String> locations;

    public DalObscuraPlannedPartition(String ticket, List<String> locations) {
        this.ticket = ticket;
        this.locations = locations;
    }

    public DalObscuraPlannedPartition(String ticket) {
        this(ticket, List.of());
    }

    public String ticket() {
        return ticket;
    }

    public List<String> locations() {
        return locations;
    }
}
