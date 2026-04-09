package io.dalobscura.connectors.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.dalobscura.connectors.testkit.FixtureBundle;
import io.dalobscura.connectors.testkit.FixtureBuilderRunner;
import io.dalobscura.connectors.testkit.LocalDalObscuraServer;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

class SparkReadIT {
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

    @Test
    void readsFilteredRowsThroughTheConnector() throws Exception {
        FixtureBundle bundle = FixtureBuilderRunner.build();
        try (LocalDalObscuraServer server = LocalDalObscuraServer.start(bundle);
                SparkSession spark =
                        SparkSession.builder()
                                .master("local[2]")
                                .appName("spark-read-it")
                                .config("spark.ui.enabled", "false")
                                .config("spark.driver.extraJavaOptions", SPARK_ARROW_JAVA_OPTS)
                                .config("spark.executor.extraJavaOptions", SPARK_ARROW_JAVA_OPTS)
                                .getOrCreate()) {
            Dataset<Row> rows =
                    spark.read()
                            .format("dal_obscura")
                            .option("dal.uri", server.uri())
                            .option("dal.catalog", bundle.catalog())
                            .option("dal.target", bundle.target())
                            .option("dal.auth.token", bundle.userToken())
                            .load()
                            .select("id", "region")
                            .filter("region = 'us'");

            assertEquals(
                    List.of(2L, 4L),
                    rows.orderBy("id").select("id").as(Encoders.LONG()).collectAsList());
        }
    }

    @Test
    void failsClearlyWhenTheTokenIsMissing() throws Exception {
        FixtureBundle bundle = FixtureBuilderRunner.build();
        try (LocalDalObscuraServer server = LocalDalObscuraServer.start(bundle);
                SparkSession spark =
                        SparkSession.builder()
                                .master("local[2]")
                                .appName("spark-auth-it")
                                .config("spark.ui.enabled", "false")
                                .config("spark.driver.extraJavaOptions", SPARK_ARROW_JAVA_OPTS)
                                .config("spark.executor.extraJavaOptions", SPARK_ARROW_JAVA_OPTS)
                                .getOrCreate()) {
            Exception error =
                    assertThrows(
                            Exception.class,
                            () ->
                                    spark.read()
                                            .format("dal_obscura")
                                            .option("dal.uri", server.uri())
                                            .option("dal.catalog", bundle.catalog())
                                            .option("dal.target", bundle.target())
                                            .load()
                                            .count());

            String message = error.getMessage() == null ? "" : error.getMessage();
            org.junit.jupiter.api.Assertions.assertTrue(
                    message.contains("Missing required option: dal.auth.token"));
        }
    }
}
