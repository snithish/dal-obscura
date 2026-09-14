package io.dalobscura.connectors.testkit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class LocalDalObscuraServer implements AutoCloseable {
    private static final Duration STARTUP_TIMEOUT = Duration.ofSeconds(20);

    private final Process process;
    private final String uri;
    private final Path logPath;
    private final JwksServer jwksServer;

    private LocalDalObscuraServer(
            Process process, String uri, Path logPath, JwksServer jwksServer) {
        this.process = process;
        this.uri = uri;
        this.logPath = logPath;
        this.jwksServer = jwksServer;
    }

    public static LocalDalObscuraServer start(FixtureBundle bundle) throws Exception {
        Path databasePath = Path.of(URI.create(bundle.databaseUrl()).getPath());
        Path logPath = databasePath.getParent().resolve("dal-obscura.log");
        Path fixtureJwksPath = databasePath.getParent().resolve("fixture-jwks.json");
        JwksServer jwksServer =
                bundle.jwksPort() > 0
                        ? JwksServer.start(bundle.jwksPort(), fixtureJwksPath)
                        : null;

        ProcessBuilder builder = new ProcessBuilder("uv", "run", "dal-obscura");
        builder.directory(FixtureBuilderRunner.workspaceRoot().toFile());
        builder.redirectErrorStream(true);
        builder.redirectOutput(logPath.toFile());

        Map<String, String> environment = builder.environment();
        environment.put("DAL_OBSCURA_DATABASE_URL", bundle.databaseUrl());
        environment.put("DAL_OBSCURA_CELL_ID", bundle.cellId());
        environment.put("DAL_OBSCURA_LOCATION", flightLocation(bundle.uri()));
        environment.put("DAL_OBSCURA_JWT_SECRET", bundle.jwtSecret());
        environment.put("DAL_OBSCURA_TICKET_SECRET", bundle.ticketSecret());

        try {
            Process process = builder.start();
            waitUntilReady(process, bundle.uri(), logPath);
            return new LocalDalObscuraServer(process, bundle.uri(), logPath, jwksServer);
        } catch (Exception error) {
            if (jwksServer != null) {
                jwksServer.close();
            }
            throw error;
        }
    }

    public String uri() {
        return uri;
    }

    @Override
    public void close() {
        if (jwksServer != null) {
            jwksServer.close();
        }
        process.destroy();
        try {
            if (!process.waitFor(5, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                process.waitFor(5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
        }
    }

    private static void waitUntilReady(Process process, String uri, Path logPath) throws Exception {
        URI parsed = URI.create(uri);
        long deadline = System.nanoTime() + STARTUP_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            if (!process.isAlive()) {
                throw new IllegalStateException(
                        "dal-obscura exited before startup completed:\n" + readLog(logPath));
            }

            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(parsed.getHost(), parsed.getPort()), 200);
                return;
            } catch (IOException ignored) {
                Thread.sleep(100L);
            }
        }

        process.destroyForcibly();
        throw new IllegalStateException(
                "Timed out waiting for dal-obscura to accept connections:\n" + readLog(logPath));
    }

    private static String flightLocation(String uri) {
        URI parsed = URI.create(uri);
        return "grpc://0.0.0.0:" + parsed.getPort();
    }

    private static String readLog(Path logPath) throws IOException {
        if (!Files.exists(logPath)) {
            return "<no log output captured>";
        }
        return Files.readString(logPath, StandardCharsets.UTF_8);
    }

    /** Minimal loopback-only HTTP server for the fixture's public JWKS document. */
    private static final class JwksServer implements AutoCloseable {
        private final ServerSocket socket;
        private final Path jwksPath;
        private final Thread thread;
        private volatile boolean running = true;

        private JwksServer(ServerSocket socket, Path jwksPath) {
            this.socket = socket;
            this.jwksPath = jwksPath;
            this.thread = new Thread(this::serve, "dal-obscura-fixture-jwks");
            this.thread.setDaemon(true);
        }

        private static JwksServer start(int port, Path jwksPath) throws IOException {
            ServerSocket socket = new ServerSocket();
            socket.setReuseAddress(true);
            socket.bind(new InetSocketAddress("127.0.0.1", port));
            JwksServer server = new JwksServer(socket, jwksPath);
            server.thread.start();
            return server;
        }

        private void serve() {
            while (running) {
                try (Socket client = socket.accept()) {
                    handle(client);
                } catch (IOException ignored) {
                    if (running) {
                        // A transient client or read failure must not stop the fixture server.
                    }
                }
            }
        }

        private void handle(Socket client) throws IOException {
            client.setSoTimeout(2_000);
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(client.getInputStream(), StandardCharsets.UTF_8));
            String requestLine = reader.readLine();
            if (requestLine == null) {
                return;
            }
            String headerLine;
            int headerCount = 0;
            while ((headerLine = reader.readLine()) != null && !headerLine.isEmpty()) {
                if (++headerCount > 64) {
                    writeResponse(client, 400, "text/plain", new byte[0]);
                    return;
                }
            }

            String[] requestParts = requestLine.split(" ", 3);
            String method = requestParts.length > 0 ? requestParts[0] : "";
            String path = requestParts.length > 1 ? requestParts[1] : "";
            if (!"GET".equals(method)) {
                writeResponse(client, 405, "text/plain", new byte[0]);
                return;
            }
            if (!"/jwks.json".equals(path)) {
                writeResponse(client, 404, "text/plain", new byte[0]);
                return;
            }

            byte[] payload;
            try {
                payload = Files.readAllBytes(jwksPath);
            } catch (IOException error) {
                writeResponse(client, 503, "text/plain", new byte[0]);
                return;
            }
            writeResponse(client, 200, "application/json", payload);
        }

        private static void writeResponse(Socket client, int status, String contentType, byte[] payload)
                throws IOException {
            String reason;
            switch (status) {
                case 200:
                    reason = "OK";
                    break;
                case 400:
                    reason = "Bad Request";
                    break;
                case 404:
                    reason = "Not Found";
                    break;
                case 405:
                    reason = "Method Not Allowed";
                    break;
                default:
                    reason = "Service Unavailable";
                    break;
            }
            String headers =
                    "HTTP/1.1 "
                            + status
                            + " "
                            + reason
                            + "\r\nContent-Type: "
                            + contentType
                            + "\r\nContent-Length: "
                            + payload.length
                            + "\r\nConnection: close\r\n\r\n";
            OutputStream output = client.getOutputStream();
            output.write(headers.getBytes(StandardCharsets.US_ASCII));
            output.write(payload);
            output.flush();
        }

        @Override
        public void close() {
            running = false;
            try {
                socket.close();
            } catch (IOException ignored) {
            }
            try {
                thread.join(2_000L);
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
