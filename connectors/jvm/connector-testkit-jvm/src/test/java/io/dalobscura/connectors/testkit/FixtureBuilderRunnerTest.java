package io.dalobscura.connectors.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FixtureBuilderRunnerTest {
    @TempDir
    Path scratch;

    @Test
    void interruptionStopsTheBuilderAndRemovesItsFixtureDirectory() throws Exception {
        Path workspace = Files.createDirectory(scratch.resolve("workspace"));
        Files.writeString(workspace.resolve("pyproject.toml"), "");
        Path bin = Files.createDirectory(workspace.resolve("bin"));
        Path uv = bin.resolve("uv");
        Files.writeString(uv, "#!/bin/sh\nprintf '%s' \"$$\" > builder.pid.tmp\nmv builder.pid.tmp builder.pid\nexec /bin/sleep 120\n");
        assertTrue(uv.toFile().setExecutable(true));
        Path output = Files.createDirectory(scratch.resolve("output"));
        ProcessBuilder builder = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Djava.io.tmpdir=" + output,
                "-cp", System.getProperty("surefire.test.class.path"),
                FixtureBuilderRunnerTest.class.getName(), "builder-interruption-probe");
        builder.directory(workspace.toFile());
        builder.environment().put("PATH", bin + ":" + builder.environment().get("PATH"));
        builder.redirectErrorStream(true);
        Process process = builder.start();
        try {
            assertTrue(process.waitFor(30, TimeUnit.SECONDS), "Builder interruption probe did not stop");
            assertEquals(0, process.exitValue(), new String(process.getInputStream().readAllBytes()));
            try (Stream<Path> entries = Files.list(output)) {
                assertEquals(List.of(), entries.map(Path::getFileName).collect(Collectors.toList()));
            }
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                process.waitFor();
            }
            Path marker = workspace.resolve("builder.pid");
            if (Files.exists(marker)) {
                ProcessHandle.of(Long.parseLong(Files.readString(marker))).ifPresent(ProcessHandle::destroyForcibly);
            }
        }
    }

    @Test
    void removesFixtureDirectoryWhenTheBuilderProcessFails() throws Exception {
        Path workspace = Files.createDirectory(scratch.resolve("workspace"));
        Files.writeString(workspace.resolve("pyproject.toml"), "");
        Path output = Files.createDirectory(scratch.resolve("output"));
        ProcessBuilder builder = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Djava.io.tmpdir=" + output,
                "-cp", System.getProperty("surefire.test.class.path"),
                FixtureBuilderRunnerTest.class.getName(), "builder-failure-probe");
        builder.directory(workspace.toFile());
        builder.environment().put("UV_CACHE_DIR", scratch.resolve("uv-cache").toString());
        builder.redirectErrorStream(true);
        Process process = builder.start();
        try {
            assertTrue(process.waitFor(30, TimeUnit.SECONDS), "Builder failure probe did not stop");
            assertEquals(0, process.exitValue(), new String(process.getInputStream().readAllBytes()));
            try (Stream<Path> entries = Files.list(output)) {
                assertEquals(List.of(), entries.map(Path::getFileName).collect(Collectors.toList()));
            }
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                process.waitFor();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (List.of(args).equals(List.of("builder-interruption-probe"))) {
            runInterruptionProbe();
            return;
        }
        assertEquals(List.of("builder-failure-probe"), List.of(args));
        IllegalStateException failure = assertThrows(IllegalStateException.class, FixtureBuilderRunner::build);
        assertTrue(failure.getMessage().startsWith("Fixture builder failed:"));
    }

    private static void runInterruptionProbe() throws Exception {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread worker = new Thread(() -> {
            try (FixtureBundle bundle = FixtureBuilderRunner.build()) {
                failure.set(new AssertionError("Builder unexpectedly completed"));
            } catch (Throwable caught) {
                failure.set(caught);
            }
        }, "fixture-builder-probe");
        worker.setDaemon(true);
        Long pid = null;
        try (WatchService watcher = Path.of(".").getFileSystem().newWatchService()) {
            Path.of(".").register(watcher, StandardWatchEventKinds.ENTRY_CREATE);
            worker.start();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            boolean ready = false;
            while (!ready) {
                long remaining = deadline - System.nanoTime();
                assertTrue(remaining > 0, "Builder marker was not created");
                WatchKey key = watcher.poll(remaining, TimeUnit.NANOSECONDS);
                assertNotNull(key, "Builder marker was not created");
                ready = key.pollEvents().stream().anyMatch(event -> event.context().toString().equals("builder.pid"));
                key.reset();
            }
            pid = Long.parseLong(Files.readString(Path.of("builder.pid")));
            worker.interrupt();
            worker.join(5000);
            assertFalse(worker.isAlive(), "Interrupted builder did not stop");
            assertTrue(failure.get() instanceof InterruptedException, "Builder did not preserve interruption");
            assertFalse(ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false), "Builder child remains running");
        } finally {
            worker.interrupt();
            if (pid != null) ProcessHandle.of(pid).ifPresent(ProcessHandle::destroyForcibly);
        }
    }

    @Test
    void rejectsMalformedExpectedMetadataInsteadOfCoercingIt() throws Exception {
        String payload =
                "{"
                        + "\"uri\":\"grpc+tcp://localhost:31337\","
                        + "\"jwks_port\":31338,"
                        + "\"catalog\":\"spark_catalog\","
                        + "\"target\":\"default.complex_users\","
                        + "\"user_token\":\"token\","
                        + "\"database_url\":\"sqlite+pysqlite:////tmp/control-plane.db\","
                        + "\"jwt_secret\":\"jwt\","
                        + "\"ticket_secret\":\"ticket\","
                        + "\"expected\":{"
                        + "\"row_count\":\"125000\","
                        + "\"supports_multiple_tickets\":true,"
                        + "\"sample_us_even_ids\":[0,4,8],"
                        + "\"masked_zip_hash_length\":64"
                        + "}"
                        + "}";

        assertThrows(
                IllegalStateException.class,
                () -> FixtureBuilderRunner.parseFixturePayload(payload, null),
                "strict parser should reject string row_count");
    }

    @Test
    void exposesExpectedMetadataFromThePythonFixturePayload() throws Exception {
        try (FixtureBundle bundle = FixtureBuilderRunner.build()) {
            assertTrue(bundle.databaseUrl().startsWith("sqlite+pysqlite:///"));
            assertTrue(bundle.jwksPort() > 0);
            assertEquals(125_000L, bundle.expectedRowCount());
            assertTrue(bundle.supportsMultipleTickets());
            assertEquals(List.of(0L, 4L, 8L), bundle.sampleUsEvenIds());
            assertEquals(64, bundle.maskedZipHashLength());
        }
    }
}
