package io.dalobscura.connectors.testkit;

import java.nio.file.Path;
import java.util.List;

public final class FixtureBundle {
    private final String uri;
    private final String catalog;
    private final String target;
    private final String userToken;
    private final Path appPath;
    private final String jwtSecret;
    private final String ticketSecret;
    private final long expectedRowCount;
    private final boolean supportsMultipleTickets;
    private final List<Long> sampleUsEvenIds;
    private final int maskedZipHashLength;

    public FixtureBundle(
            String uri,
            String catalog,
            String target,
            String userToken,
            Path appPath,
            String jwtSecret,
            String ticketSecret) {
        this(uri, catalog, target, userToken, appPath, jwtSecret, ticketSecret, -1L, false, List.of(), -1);
    }

    public FixtureBundle(
            String uri,
            String catalog,
            String target,
            String userToken,
            Path appPath,
            String jwtSecret,
            String ticketSecret,
            long expectedRowCount,
            boolean supportsMultipleTickets,
            List<Long> sampleUsEvenIds,
            int maskedZipHashLength) {
        this.uri = uri;
        this.catalog = catalog;
        this.target = target;
        this.userToken = userToken;
        this.appPath = appPath;
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

    public String catalog() {
        return catalog;
    }

    public String target() {
        return target;
    }

    public String userToken() {
        return userToken;
    }

    public Path appPath() {
        return appPath;
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
}
