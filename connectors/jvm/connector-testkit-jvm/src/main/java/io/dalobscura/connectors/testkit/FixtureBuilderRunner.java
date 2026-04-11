package io.dalobscura.connectors.testkit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public final class FixtureBuilderRunner {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private FixtureBuilderRunner() {}

    public static FixtureBundle build() throws Exception {
        Path outputDir = Files.createTempDirectory("spark-connector-fixture");
        int port = reservePort();

        ProcessBuilder builder =
                new ProcessBuilder(
                        "uv",
                        "run",
                        "tests/support/build_connector_fixture.py",
                        "--output-dir",
                        outputDir.toString(),
                        "--port",
                        Integer.toString(port));
        builder.directory(workspaceRoot().toFile());
        builder.redirectErrorStream(true);

        Process process = builder.start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new IllegalStateException("Fixture builder failed:\n" + output);
        }

        JsonNode node = MAPPER.readTree(extractJsonPayload(output));
        JsonNode expected = node.path("expected");
        return new FixtureBundle(
                node.get("uri").asText(),
                node.get("catalog").asText(),
                node.get("target").asText(),
                node.get("user_token").asText(),
                Path.of(node.get("app_path").asText()),
                node.get("jwt_secret").asText(),
                node.get("ticket_secret").asText(),
                expected.path("row_count").asLong(),
                expected.path("supports_multiple_tickets").asBoolean(),
                readSampleUsEvenIds(expected),
                expected.path("masked_zip_hash_length").asInt());
    }

    static Path workspaceRoot() throws IOException {
        Path current = Path.of("").toAbsolutePath();
        while (current != null) {
            if (Files.exists(current.resolve("pyproject.toml"))) {
                return current;
            }
            current = current.getParent();
        }
        throw new IOException("Could not locate repository root from current working directory");
    }

    private static int reservePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static String extractJsonPayload(String output) {
        String[] lines = output.split("\\R");
        for (int index = lines.length - 1; index >= 0; index--) {
            String candidate = lines[index].trim();
            if (candidate.startsWith("{") && candidate.endsWith("}")) {
                return candidate;
            }
        }
        throw new IllegalStateException("Fixture builder did not emit JSON:\n" + output);
    }

    private static List<Long> readSampleUsEvenIds(JsonNode expected) {
        JsonNode ids = expected.path("sample_us_even_ids");
        if (!ids.isArray() || ids.isEmpty()) {
            throw new IllegalStateException("Fixture expected sample_us_even_ids metadata");
        }

        List<Long> result = new ArrayList<>(ids.size());
        for (JsonNode id : ids) {
            result.add(id.asLong());
        }
        return result;
    }
}
