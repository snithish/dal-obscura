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

        return parseFixturePayload(extractJsonPayload(output));
    }

    static FixtureBundle parseFixturePayload(String payload) throws IOException {
        JsonNode node = MAPPER.readTree(payload);
        JsonNode expected = requireObject(node, "expected");
        return new FixtureBundle(
                requireText(node, "uri"),
                requireText(node, "catalog"),
                requireText(node, "target"),
                requireText(node, "user_token"),
                Path.of(requireText(node, "app_path")),
                requireText(node, "jwt_secret"),
                requireText(node, "ticket_secret"),
                requireLong(expected, "row_count"),
                requireBoolean(expected, "supports_multiple_tickets"),
                requireLongArray(expected, "sample_us_even_ids"),
                requireInt(expected, "masked_zip_hash_length"));
    }

    private static JsonNode requireObject(JsonNode node, String fieldName) {
        JsonNode value = requireField(node, fieldName);
        if (!value.isObject()) {
            throw new IllegalStateException("Expected field '" + fieldName + "' to be an object");
        }
        return value;
    }

    private static String requireText(JsonNode node, String fieldName) {
        JsonNode value = requireField(node, fieldName);
        if (!value.isTextual()) {
            throw new IllegalStateException("Expected field '" + fieldName + "' to be a string");
        }
        return value.asText();
    }

    private static long requireLong(JsonNode node, String fieldName) {
        JsonNode value = requireField(node, fieldName);
        if (!value.isIntegralNumber()) {
            throw new IllegalStateException("Expected field '" + fieldName + "' to be an integer");
        }
        return value.longValue();
    }

    private static int requireInt(JsonNode node, String fieldName) {
        long value = requireLong(node, fieldName);
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
            throw new IllegalStateException("Expected field '" + fieldName + "' to fit in int");
        }
        return (int) value;
    }

    private static boolean requireBoolean(JsonNode node, String fieldName) {
        JsonNode value = requireField(node, fieldName);
        if (!value.isBoolean()) {
            throw new IllegalStateException("Expected field '" + fieldName + "' to be a boolean");
        }
        return value.booleanValue();
    }

    private static List<Long> requireLongArray(JsonNode node, String fieldName) {
        JsonNode value = requireField(node, fieldName);
        if (!value.isArray()) {
            throw new IllegalStateException("Expected field '" + fieldName + "' to be an array");
        }
        List<Long> result = new ArrayList<>(value.size());
        for (JsonNode element : value) {
            if (!element.isIntegralNumber()) {
                throw new IllegalStateException(
                        "Expected field '" + fieldName + "' to contain integer values");
            }
            result.add(element.longValue());
        }
        return result;
    }

    private static JsonNode requireField(JsonNode node, String fieldName) {
        JsonNode value = node.get(fieldName);
        if (value == null || value.isNull()) {
            throw new IllegalStateException("Missing required field '" + fieldName + "'");
        }
        return value;
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

}
