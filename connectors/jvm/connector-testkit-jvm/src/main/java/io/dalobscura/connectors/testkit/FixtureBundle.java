package io.dalobscura.connectors.testkit;

import java.nio.file.Path;

public final class FixtureBundle {
    private final String uri;
    private final String catalog;
    private final String target;
    private final String userToken;
    private final Path appPath;
    private final String jwtSecret;
    private final String ticketSecret;

    public FixtureBundle(
            String uri,
            String catalog,
            String target,
            String userToken,
            Path appPath,
            String jwtSecret,
            String ticketSecret) {
        this.uri = uri;
        this.catalog = catalog;
        this.target = target;
        this.userToken = userToken;
        this.appPath = appPath;
        this.jwtSecret = jwtSecret;
        this.ticketSecret = ticketSecret;
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
}
