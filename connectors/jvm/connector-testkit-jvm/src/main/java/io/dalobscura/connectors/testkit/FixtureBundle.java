package io.dalobscura.connectors.testkit;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class FixtureBundle implements AutoCloseable {
    private final Path outputDirectory;
    private final String uri;
    private final int jwksPort;
    private final String catalog;
    private final String target;
    private final String userToken;
    private final String databaseUrl;
    private final String jwtSecret;
    private final String ticketSecret;
    private final long expectedRowCount;
    private final boolean supportsMultipleTickets;
    private final List<Long> sampleUsEvenIds;
    private final int maskedZipHashLength;

    FixtureBundle(
            String uri,
            int jwksPort,
            String catalog,
            String target,
            String userToken,
            String databaseUrl,
            String jwtSecret,
            String ticketSecret,
            long expectedRowCount,
            boolean supportsMultipleTickets,
            List<Long> sampleUsEvenIds,
            int maskedZipHashLength,
            Path outputDirectory) {
        this.outputDirectory = outputDirectory;
        this.uri = uri;
        this.jwksPort = jwksPort;
        this.catalog = catalog;
        this.target = target;
        this.userToken = userToken;
        this.databaseUrl = databaseUrl;
        this.jwtSecret = jwtSecret;
        this.ticketSecret = ticketSecret;
        this.expectedRowCount = expectedRowCount;
        this.supportsMultipleTickets = supportsMultipleTickets;
        this.sampleUsEvenIds = List.copyOf(sampleUsEvenIds);
        this.maskedZipHashLength = maskedZipHashLength;
    }

    public String uri() {
        return uri;
    }

    public int jwksPort() {
        return jwksPort;
    }

    public String catalog() {
        return catalog;
    }

    public String target() {
        return target;
    }

    public String userToken() {
        return userToken;
    }

    public String databaseUrl() {
        return databaseUrl;
    }

    public String jwtSecret() {
        return jwtSecret;
    }

    public String ticketSecret() {
        return ticketSecret;
    }

    public long expectedRowCount() {
        return expectedRowCount;
    }

    public boolean supportsMultipleTickets() {
        return supportsMultipleTickets;
    }

    public List<Long> sampleUsEvenIds() {
        return sampleUsEvenIds;
    }

    public int maskedZipHashLength() {
        return maskedZipHashLength;
    }

    @Override
    public void close() throws IOException {
        deleteDirectory(outputDirectory);
    }

    static void deleteDirectory(Path directory) throws IOException {
        if (directory == null || !Files.exists(directory)) return;
        List<Path> entries;
        try (Stream<Path> paths = Files.walk(directory)) {
            entries = paths.sorted(Comparator.reverseOrder()).collect(Collectors.toList());
        }
        for (Path entry : entries) Files.deleteIfExists(entry);
    }
}
