package io.dalobscura.connectors.integration;

import io.dalobscura.connectors.testkit.FixtureBundle;
import io.dalobscura.connectors.testkit.FixtureBuilderRunner;
import io.dalobscura.connectors.testkit.LocalDalObscuraServer;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

final class SparkFixture implements AutoCloseable {
    private static final String EXECUTOR_TOKEN_PROPERTY = "dal.obscura.integration.executor.token";
    private static final String SPARK_ARROW_JAVA_OPTS =
            "-XX:+IgnoreUnrecognizedVMOptions "
                    + "--add-opens=java.base/java.lang=ALL-UNNAMED "
                    + "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
                    + "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
                    + "--add-opens=java.base/java.io=ALL-UNNAMED "
                    + "--add-opens=java.base/java.net=ALL-UNNAMED "
                    + "--add-opens=java.base/java.nio=ALL-UNNAMED "
                    + "--add-opens=java.base/java.util=ALL-UNNAMED "
                    + "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
                    + "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
                    + "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED "
                    + "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
                    + "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
                    + "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
                    + "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
                    + "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED "
                    + "-Djdk.reflect.useDirectMethodHandle=false "
                    + "-Dio.netty.tryReflectionSetAccessible=true";

    private final FixtureBundle bundle;
    private final LocalDalObscuraServer server;
    private final SparkSession spark;
    private final String previousExecutorToken;

    private SparkFixture(
            FixtureBundle bundle,
            LocalDalObscuraServer server,
            SparkSession spark,
            String previousExecutorToken) {
        this.bundle = bundle;
        this.server = server;
        this.spark = spark;
        this.previousExecutorToken = previousExecutorToken;
    }

    static SparkFixture create(String appName) throws Exception {
        FixtureBundle bundle = FixtureBuilderRunner.build();
        LocalDalObscuraServer server = LocalDalObscuraServer.start(bundle);
        String previousToken = System.getProperty(EXECUTOR_TOKEN_PROPERTY);
        System.setProperty(EXECUTOR_TOKEN_PROPERTY, bundle.userToken());
        try {
            SparkSession spark =
                    SparkSession.builder()
                            .master("local[2]")
                            .appName(appName)
                            .config("spark.ui.enabled", "false")
                            .config("spark.driver.extraJavaOptions", SPARK_ARROW_JAVA_OPTS)
                            .config("spark.executor.extraJavaOptions", SPARK_ARROW_JAVA_OPTS)
                            .getOrCreate();
            return new SparkFixture(bundle, server, spark, previousToken);
        } catch (RuntimeException | Error failure) {
            restoreExecutorToken(previousToken);
            try {
                server.close();
            } catch (Exception cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    FixtureBundle bundle() {
        return bundle;
    }

    Dataset<Row> read() {
        return reader(true).load();
    }

    Dataset<Row> readWithAuthorizationHeader() {
        return readerWithAuthorizationHeader().load();
    }

    Dataset<Row> readWithoutToken() {
        return reader(false).load();
    }

    private DataFrameReader reader(boolean includeToken) {
        DataFrameReader reader =
                spark.read()
                        .format("dal_obscura")
                        .option("dal.uri", server.uri())
                        .option("dal.catalog", bundle.catalog())
                        .option("dal.target", bundle.target())
                        .option("dal.executor.auth.token-property", EXECUTOR_TOKEN_PROPERTY);
        if (includeToken) {
            reader = reader.option("dal.auth.token-property", EXECUTOR_TOKEN_PROPERTY);
        }
        return reader;
    }

    private DataFrameReader readerWithAuthorizationHeader() {
        return spark.read()
                .format("dal_obscura")
                .option("dal.uri", server.uri())
                .option("dal.catalog", bundle.catalog())
                .option("dal.target", bundle.target())
                .option("dal.executor.auth.token-property", EXECUTOR_TOKEN_PROPERTY)
                .option("dal.auth.header.authorization", "Bearer " + bundle.userToken());
    }

    private static void restoreExecutorToken(String previousToken) {
        if (previousToken == null) {
            System.clearProperty(EXECUTOR_TOKEN_PROPERTY);
        } else {
            System.setProperty(EXECUTOR_TOKEN_PROPERTY, previousToken);
        }
    }

    @Override
    public void close() throws Exception {
        try {
            spark.close();
        } finally {
            restoreExecutorToken(previousExecutorToken);
            server.close();
        }
    }
}
