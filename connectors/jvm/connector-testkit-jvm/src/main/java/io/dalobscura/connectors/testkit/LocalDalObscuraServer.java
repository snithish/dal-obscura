package io.dalobscura.connectors.testkit;

import java.io.IOException;
import java.net.InetSocketAddress;
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

    private LocalDalObscuraServer(Process process, String uri, Path logPath) {
        this.process = process;
        this.uri = uri;
        this.logPath = logPath;
    }

    public static LocalDalObscuraServer start(FixtureBundle bundle) throws Exception {
        Path logPath = bundle.appPath().getParent().resolve("dal-obscura.log");

        ProcessBuilder builder =
                new ProcessBuilder(
                        "uv",
                        "run",
                        "dal-obscura",
                        "--app-config",
                        bundle.appPath().toString());
        builder.directory(FixtureBuilderRunner.workspaceRoot().toFile());
        builder.redirectErrorStream(true);
        builder.redirectOutput(logPath.toFile());

        Map<String, String> environment = builder.environment();
        environment.put("DAL_OBSCURA_JWT_SECRET", bundle.jwtSecret());
        environment.put("DAL_OBSCURA_TICKET_SECRET", bundle.ticketSecret());

        Process process = builder.start();
        waitUntilReady(process, bundle.uri(), logPath);
        return new LocalDalObscuraServer(process, bundle.uri(), logPath);
    }

    public String uri() {
        return uri;
    }

    @Override
    public void close() {
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

    private static String readLog(Path logPath) throws IOException {
        if (!Files.exists(logPath)) {
            return "<no log output captured>";
        }
        return Files.readString(logPath, StandardCharsets.UTF_8);
    }
}
