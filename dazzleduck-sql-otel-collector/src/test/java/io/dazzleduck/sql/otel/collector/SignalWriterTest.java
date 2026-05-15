package io.dazzleduck.sql.otel.collector;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.ingestion.DuckLakeIngestionHandler;
import io.dazzleduck.sql.commons.ingestion.IngestionConfig;
import io.dazzleduck.sql.commons.ingestion.QueueIdToTableMapping;
import io.dazzleduck.sql.commons.util.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for SignalWriter transformation and partition behavior.
 *
 * <p>All per-signal settings (target path, transformation, partition columns) come from
 * {@link DuckLakeIngestionHandler} via the queue mapping. Queue tuning parameters
 * (bucket size, flush delay) come from {@link IngestionConfig}.
 */
class SignalWriterTest {

    private static final String CATALOG  = "signal_writer_test";
    private static final String QUEUE_ID = "logs";

    /** Fast-flush config: minBucketSize=1 so any batch triggers an immediate write. */
    private static final IngestionConfig FAST_CONFIG = new IngestionConfig(
            1L, IngestionConfig.DEFAULT_MAX_BUCKET_SIZE, IngestionConfig.DEFAULT_MAX_BATCHES,
            IngestionConfig.DEFAULT_MAX_PENDING_WRITE, Duration.ofSeconds(10),
            IngestionConfig.DEFAULT_CONFIG_REFRESH);

    @TempDir
    Path tempDir;

    @BeforeEach
    void setupDuckLake() throws Exception {
        ConnectionPool.executeBatch(new String[]{
                "INSTALL ducklake", "LOAD ducklake",
                "INSTALL arrow FROM community", "LOAD arrow"
        });
        Path dataPath = tempDir.resolve("data");
        Files.createDirectories(dataPath);
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn,
                    "ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s')"
                            .formatted(tempDir.resolve("catalog"), CATALOG, dataPath));
        }
    }

    @AfterEach
    void detach() {
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, "DETACH " + CATALOG);
        } catch (Exception ignored) {}
    }

    // -----------------------------------------------------------------------
    // Transformation tests
    // -----------------------------------------------------------------------

    /**
     * Transformation in the handler mapping is applied at write time.
     * An additive expression adds a derived column ({@code msg_len}) to the DuckLake table.
     */
    @Test
    void transformation_appliedFromHandlerMapping() throws Exception {
        createTable("logs (level VARCHAR, msg VARCHAR, msg_len BIGINT)");

        var handler = handler("SELECT *, length(msg) as msg_len FROM __this");

        try (var writer = new SignalWriter(QUEUE_ID, handler, FAST_CONFIG)) {
            writer.addBatch(arrowFile("SELECT 'INFO' AS level, 'hello' AS msg"))
                  .get(10, TimeUnit.SECONDS);
        }

        TestUtils.isEqual(
                "SELECT 'INFO' AS level, 'hello' AS msg, 5 AS msg_len",
                "SELECT level, msg, msg_len FROM %s.main.logs".formatted(CATALOG));
    }

    /**
     * A full SQL replacement transformation lowers the level column.
     */
    @Test
    void handlerTransformation_fullSqlReplacement() throws Exception {
        createTable("logs (level VARCHAR, msg VARCHAR)");

        var handler = handler("SELECT lower(level) AS level, msg FROM __this");

        try (var writer = new SignalWriter(QUEUE_ID, handler, FAST_CONFIG)) {
            writer.addBatch(arrowFile("SELECT 'INFO' AS level, 'hello' AS msg"))
                  .get(10, TimeUnit.SECONDS);
        }

        TestUtils.isEqual(
                "SELECT 'info' AS level, 'hello' AS msg",
                "SELECT level, msg FROM %s.main.logs".formatted(CATALOG));
    }

    // -----------------------------------------------------------------------
    // Partition tests
    // -----------------------------------------------------------------------

    /**
     * When the DuckLake table has partition columns, they are used by the handler.
     * Verifies Hive-style subdirectory structure at the DuckLake table's target path.
     */
    @Test
    void partitionBy_fromDuckLakeTableMetadata() throws Exception {
        createTable("logs (level VARCHAR, msg VARCHAR)");
        ConnectionPool.execute("ALTER TABLE %s.main.logs SET PARTITIONED BY (level)".formatted(CATALOG));

        var handler = handler(null);

        try (var writer = new SignalWriter(QUEUE_ID, handler, FAST_CONFIG)) {
            writer.addBatch(arrowFile("SELECT 'INFO' AS level, 'hello' AS msg"))
                  .get(10, TimeUnit.SECONDS);
        }

        Path tablePath = Path.of(handler.getTargetPath(QUEUE_ID));
        assertTrue(
                Files.walk(tablePath, 1).anyMatch(p -> p.getFileName().toString().startsWith("level=")),
                "Expected DuckLake partition dir 'level=...' under " + tablePath);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private void createTable(String ddl) throws Exception {
        ConnectionPool.execute("CREATE TABLE %s.main.%s".formatted(CATALOG, ddl));
    }

    /**
     * Creates a {@link DuckLakeIngestionHandler} mapping QUEUE_ID to the logs table.
     * Target path and partition columns come from DuckLake metadata; transformation is optional.
     */
    private DuckLakeIngestionHandler handler(String transformation) {
        return new DuckLakeIngestionHandler(Map.of(
                QUEUE_ID, new QueueIdToTableMapping(
                        QUEUE_ID, CATALOG, "main", "logs", Map.of(), transformation)));
    }

    private Path arrowFile(String sql) throws Exception {
        Path file = tempDir.resolve("batch_" + System.nanoTime() + ".arrow");
        ConnectionPool.execute("COPY (%s) TO '%s' (FORMAT arrow)".formatted(sql, file));
        return file;
    }
}
