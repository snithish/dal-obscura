package io.dalobscura.connectors.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

public final class FlightDalObscuraReadClient implements DalObscuraReadClient {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    private final FlightClient client;

    public FlightDalObscuraReadClient(String uri) {
        this.client = FlightClient.builder(allocator, locationFor(uri)).build();
    }

    @Override
    public DalObscuraPlannedRead plan(DalObscuraPlanRequest request, String authToken) {
        FlightInfo info =
                client.getInfo(
                        FlightDescriptor.command(encodePlanCommand(request)),
                        headerOption(authToken));

        List<DalObscuraPlannedPartition> partitions =
                info.getEndpoints().stream()
                        .map(FlightDalObscuraReadClient::toPartition)
                        .collect(Collectors.toList());
        return new DalObscuraPlannedRead(info.getSchema(), partitions);
    }

    @Override
    public DalObscuraTicketStream openStream(DalObscuraPlannedPartition partition, String authToken) {
        final FlightStream stream =
                client.getStream(
                        new Ticket(partition.ticket().getBytes(StandardCharsets.UTF_8)),
                        headerOption(authToken));
        return new DalObscuraTicketStream() {
            @Override
            public boolean next() {
                return stream.next();
            }

            @Override
            public VectorSchemaRoot root() {
                return stream.getRoot();
            }

            @Override
            public void close() {
                try {
                    stream.close();
                } catch (Exception error) {
                    throw new IllegalStateException("Failed to close Flight stream", error);
                }
            }
        };
    }

    static byte[] encodePlanCommand(DalObscuraPlanRequest request) {
        try {
            LinkedHashMap<String, Object> payload = new LinkedHashMap<>();
            payload.put("catalog", request.catalog());
            payload.put("target", request.target());
            payload.put("columns", request.columns());
            if (request.rowFilter().isPresent()) {
                payload.put("row_filter", request.rowFilter().get());
            }
            return MAPPER.writeValueAsBytes(payload);
        } catch (Exception error) {
            throw new IllegalStateException("Failed to encode plan request", error);
        }
    }

    static String authorizationHeaderValue(String authToken) {
        return "Bearer " + authToken;
    }

    static HeaderCallOption headerOption(String authToken) {
        FlightCallHeaders headers = new FlightCallHeaders();
        headers.insert("authorization", authorizationHeaderValue(authToken));
        return new HeaderCallOption(headers);
    }

    static Location locationFor(String uri) {
        URI parsed = URI.create(uri);
        String scheme = parsed.getScheme();
        if ("grpc+tcp".equals(scheme)) {
            return Location.forGrpcInsecure(parsed.getHost(), parsed.getPort());
        }
        if ("grpc+tls".equals(scheme)) {
            return Location.forGrpcTls(parsed.getHost(), parsed.getPort());
        }
        throw new IllegalArgumentException("Unsupported dal.uri scheme: " + scheme);
    }

    @Override
    public void close() {
        RuntimeException failure = null;
        try {
            client.close();
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            failure = new IllegalStateException("Interrupted while closing Flight client", error);
        }
        try {
            allocator.close();
        } catch (RuntimeException error) {
            if (failure == null) {
                failure = error;
            } else {
                failure.addSuppressed(error);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static DalObscuraPlannedPartition toPartition(FlightEndpoint endpoint) {
        List<String> locations =
                endpoint.getLocations().stream()
                        .map(location -> location.getUri().toString())
                        .collect(Collectors.toList());
        return new DalObscuraPlannedPartition(
                new String(endpoint.getTicket().getBytes(), StandardCharsets.UTF_8),
                locations);
    }
}
