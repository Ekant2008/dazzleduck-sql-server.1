package io.dazzleduck.sql.otel.collector;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.auth.Validator;
import io.dazzleduck.sql.commons.ingestion.DuckLakeIngestionHandler;
import io.dazzleduck.sql.commons.ingestion.QueueIdToTableMapping;
import io.dazzleduck.sql.commons.util.TestUtils;
import io.dazzleduck.sql.otel.collector.config.CollectorProperties;
import io.dazzleduck.sql.otel.collector.config.SignalIngestionConfig;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.jsonwebtoken.Jwts;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.messages.Item;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import io.opentelemetry.proto.logs.v1.SeverityNumber;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.crypto.SecretKey;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for DuckLake with PostgreSQL metadata backend and MinIO object storage.
 *
 * <p>Validates end-to-end OpenTelemetry signal ingestion into DuckLake:
 * <ul>
 *   <li>Metadata tables (ducklake_schema, ducklake_table, ducklake_metadata, ducklake_data_file)
 *       are created directly in PostgreSQL when using postgres: connection string</li>
 *   <li>Parquet data files are written to MinIO via s3:// paths</li>
 *   <li>DuckDB can query DuckLake tables (metadata from PostgreSQL + data from MinIO)</li>
 *   <li>Parquet files contain the correct ingested data</li>
 * </ul>
 *
 * <p>Setup:
 * <ul>
 *   <li>MinIO Testcontainer for S3-compatible object storage</li>
 *   <li>PostgreSQL Testcontainer for DuckLake metadata</li>
 *   <li>DuckDB with ducklake extension, attached via postgres: connection string</li>
 *   <li>S3 secret created in DuckDB for MinIO access</li>
 * </ul>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OtelCollectorDuckLakePostgresMinioTest {

    private static final String SECRET_KEY_BASE64 =
            "VGhpcyBpcyBhIDY0IGJpdCBsb25nIGtleSB3aGljaCBzaG91bGQgYmUgY2hhbmdlZCBpbiBwcm9kdWN0aW9uLiBTbyBjaGFuZ2UgbWUgYW5kIG1ha2Ugc3VyZSBpdHMgMTI4IGJpdCBsb25nIG9yIG1vcmU";
    private static final Metadata.Key<String> AUTHORIZATION_KEY =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    private static final String CATALOG = "otel_ducklake_pg_minio_test";
    private static final String BUCKET = "test-bucket";
    private static final String PREFIX = "otel-data";
    private static final String DUCKDB_SECRET_NAME = "minio_s3";

    private MinIOContainer minio;
    private MinioClient minioClient;
    private PostgreSQLContainer<?> postgres;

    private OtelCollectorServer otelServer;
    private ManagedChannel channel;
    private LogsServiceGrpc.LogsServiceBlockingStub logsStub;

    @BeforeAll
    void setup() throws Exception {
        // Start external backends
        minio = new MinIOContainer("minio/minio:RELEASE.2023-09-04T19-57-37Z");
        minio.start();
        minioClient = MinioClient.builder()
                .endpoint(minio.getS3URL())
                .credentials(minio.getUserName(), minio.getPassword())
                .build();
        minioClient.makeBucket(MakeBucketArgs.builder().bucket(BUCKET).build());

        postgres = new PostgreSQLContainer<>("postgres:16-alpine");
        postgres.start();

        // Create temp dirs under the Maven module's target/ folder to avoid permission issues
        // with the system temp directory in some environments.
        Path tempRoot = Path.of("target");
        Files.createDirectories(tempRoot);
        Path tempDir = Files.createTempDirectory(tempRoot, "otel-ducklake-pg-minio-test");
        Path ducklakeCatalogId = tempDir.resolve("catalog");

        String dataPath = "s3://%s/%s".formatted(BUCKET, PREFIX);

        // Configure DuckDB for S3 (MinIO) and DuckLake with PostgreSQL metadata backend.
        URI minioUri = URI.create(minio.getS3URL());
        String createSecret = "CREATE SECRET %s (TYPE s3, KEY_ID '%s', SECRET '%s', ENDPOINT '%s', USE_SSL false, URL_STYLE path);"
                .formatted(DUCKDB_SECRET_NAME, minio.getUserName(), minio.getPassword(), minioUri.getHost() + ":" + minioUri.getPort());

        // DuckLake options with PostgreSQL as metadata backend:
        // - DATA_PATH points at MinIO (s3://...) for data files
        // - Use postgres: connection string for metadata storage in PostgreSQL
        // - OVERRIDE_DATA_PATH True allows overriding data path
        // Note: For Testcontainers, use the actual host and mapped port
        String ducklakePostgresConn = "ducklake:postgres:host=%s port=%d user=%s password=%s dbname=%s".formatted(
                postgres.getHost(),
                postgres.getFirstMappedPort(),
                postgres.getUsername(),
                postgres.getPassword(),
                postgres.getDatabaseName()
        );

        String attachDuckLake = "ATTACH '%s' AS %s (DATA_PATH '%s', OVERRIDE_DATA_PATH True);"
                .formatted(ducklakePostgresConn, CATALOG, dataPath);

        ConnectionPool.executeOnSingleton("""
                INSTALL arrow FROM community;
                LOAD arrow;
                INSTALL httpfs;
                LOAD httpfs;
                INSTALL ducklake;
                LOAD ducklake;
                %s
                %s
                """.formatted(createSecret, attachDuckLake));

        // Create target tables
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatchInTxn(conn, new String[]{
                    "CREATE TABLE %s.main.logs (severity_number INT, severity_text VARCHAR, body VARCHAR)".formatted(CATALOG),
                    "CREATE TABLE %s.main.spans (name VARCHAR, kind VARCHAR, duration_ms BIGINT)".formatted(CATALOG),
                    "CREATE TABLE %s.main.metrics (name VARCHAR, metric_type VARCHAR, value_double DOUBLE)".formatted(CATALOG)
            });
        }

        // Set up ingestion handler mappings (queueId suffix -> table)
        var logHandler = new DuckLakeIngestionHandler(Map.of(
                "logs", new QueueIdToTableMapping("logs", CATALOG, "main", "logs", Map.of(), null)));
        var traceHandler = new DuckLakeIngestionHandler(Map.of(
                "traces", new QueueIdToTableMapping("traces", CATALOG, "main", "spans", Map.of(), null)));
        var metricHandler = new DuckLakeIngestionHandler(Map.of(
                "metrics", new QueueIdToTableMapping("metrics", CATALOG, "main", "metrics", Map.of(), null)));

        CollectorProperties props = new CollectorProperties();
        props.setGrpcPort(freePort());
        props.setAuthentication("jwt");
        props.setSecretKey(SECRET_KEY_BASE64);
        props.setStartupScript(""); // already configured on singleton above

        // Flush quickly to reduce test flakiness
        props.setLogIngestionConfig(new SignalIngestionConfig(
                "s3://%s/%s/logs".formatted(BUCKET, PREFIX),
                List.of(), null, 1L, 2000L));
        props.setTraceIngestionConfig(new SignalIngestionConfig(
                "s3://%s/%s/traces".formatted(BUCKET, PREFIX),
                List.of(), null, 1L, 2000L));
        props.setMetricIngestionConfig(new SignalIngestionConfig(
                "s3://%s/%s/metrics".formatted(BUCKET, PREFIX),
                List.of(), null, 1L, 2000L));

        props.setLogIngestionTaskFactory(logHandler);
        props.setTraceIngestionTaskFactory(traceHandler);
        props.setMetricIngestionTaskFactory(metricHandler);

        otelServer = new OtelCollectorServer(props);
        otelServer.start();

        channel = ManagedChannelBuilder.forAddress("localhost", props.getGrpcPort()).usePlaintext().build();
        var interceptor = MetadataUtils.newAttachHeadersInterceptor(bearerMeta());
        logsStub = LogsServiceGrpc.newBlockingStub(channel).withInterceptors(interceptor);
    }

    @AfterAll
    void cleanup() {
        if (channel != null) {
            try {
                channel.shutdown();
                channel.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (otelServer != null) {
            try {
                otelServer.close();
            } catch (Exception e) {
                // Ignore cleanup exceptions
            }
        }
        try {
            ConnectionPool.execute("DETACH " + CATALOG);
        } catch (Exception e) {
            // Ignore cleanup exceptions
        }
        if (postgres != null) postgres.stop();
        if (minio != null) minio.stop();
    }

    @Test
    void logRecord_metadataInPostgres_and_dataInMinio() throws Exception {
        // Export log record via OpenTelemetry collector
        logsStub.export(createLogExportRequest());

        // Validate DuckLake table is queryable via DuckDB (end-to-end: signal -> DuckLake table)
        TestUtils.isEqual(
                "SELECT 9 AS severity_number, 'INFO' AS severity_text, 'hello pg+minio' AS body",
                "SELECT severity_number, severity_text, body FROM %s.main.logs".formatted(CATALOG));

        // Validate DuckLake metadata is stored directly in PostgreSQL
        assertPostgresHasDuckLakeMetadataDirectly();

        // Validate Parquet data in MinIO contains correct log record using DuckDB's S3 support
        verifyParquetDataInMinio();
    }

    /**
     * Creates an OpenTelemetry log export request with test data.
     */
    private ExportLogsServiceRequest createLogExportRequest() {
        return ExportLogsServiceRequest.newBuilder()
                .addResourceLogs(ResourceLogs.newBuilder()
                        .addScopeLogs(ScopeLogs.newBuilder()
                                .addLogRecords(LogRecord.newBuilder()
                                        .setSeverityNumber(SeverityNumber.SEVERITY_NUMBER_INFO)
                                        .setSeverityText("INFO")
                                        .setBody(io.opentelemetry.proto.common.v1.AnyValue.newBuilder()
                                                .setStringValue("hello pg+minio").build())
                                        .build())
                                .build())
                        .build())
                .build();
    }

    /**
     * Verifies that Parquet files exist in MinIO and contain the expected log record data.
     * Uses DuckDB's S3 support to query Parquet files directly from MinIO.
     */
    private void verifyParquetDataInMinio() throws Exception {
        boolean foundParquet = false;
        Iterable<Result<Item>> results = minioClient.listObjects(
                io.minio.ListObjectsArgs.builder()
                        .bucket(BUCKET)
                        .prefix(PREFIX + "/logs/")
                        .recursive(true)
                        .build());

        for (Result<Item> r : results) {
            Item item = r.get();
            if (item.objectName().endsWith(".parquet")) {
                String s3Path = "s3://%s/%s".formatted(BUCKET, item.objectName());
                TestUtils.isEqual(
                        "SELECT 9 AS severity_number, 'INFO' AS severity_text, 'hello pg+minio' AS body",
                        "SELECT severity_number, severity_text, body FROM read_parquet('" + s3Path + "')");
                foundParquet = true;
                break;
            }
        }
        assertTrue(foundParquet, "Expected at least one .parquet object under " + PREFIX + "/logs/");
    }

    private void assertPostgresHasDuckLakeMetadataDirectly() throws Exception {
        // DuckLake stores metadata directly in PostgreSQL when using postgres: connection string
        try (var pgConn = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             Statement st = pgConn.createStatement()) {

            // Verify DuckLake metadata tables exist in PostgreSQL public schema
            String[] expectedTables = {"ducklake_schema", "ducklake_table", "ducklake_metadata", "ducklake_data_file"};
            String tableList = String.join("', '", expectedTables);

            try (ResultSet rs = st.executeQuery(
                    "SELECT table_name FROM information_schema.tables " +
                    "WHERE table_schema = 'public' AND table_name IN ('" + tableList + "')")) {

                Map<String, Boolean> tableExists = new HashMap<>(
                        Map.of(
                                "ducklake_schema", false,
                                "ducklake_table", false,
                                "ducklake_metadata", false,
                                "ducklake_data_file", false
                        )
                );

                while (rs.next()) {
                    String tableName = rs.getString("table_name");
                    tableExists.put(tableName, true);
                }

                for (String tableName : expectedTables) {
                    assertTrue(tableExists.get(tableName),
                            "Expected " + tableName + " table in PostgreSQL");
                }
            }

            // Validate metadata tables contain expected data
            long schemaCount = count(st, "SELECT COUNT(*) FROM ducklake_schema");
            long tableCount = count(st, "SELECT COUNT(*) FROM ducklake_table");
            long metadataCount = count(st, "SELECT COUNT(*) FROM ducklake_metadata");
            long dataFileCount = count(st, "SELECT COUNT(*) FROM ducklake_data_file");

            assertTrue(schemaCount > 0, "Expected at least one row in ducklake_schema");
            assertTrue(tableCount > 0, "Expected at least one row in ducklake_table");
            assertTrue(metadataCount > 0, "Expected at least one row in ducklake_metadata");
            assertTrue(dataFileCount > 0, "Expected at least one row in ducklake_data_file");
        }
    }

    /**
     * Executes a count query and returns the result.
     *
     * @param st the statement to execute the query on
     * @param sql the SQL count query (e.g., "SELECT COUNT(*) FROM table")
     * @return the count value
     * @throws SQLException if the query fails
     */
    private static long count(Statement st, String sql) throws SQLException {
        try (ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    /**
     * Finds a free port on localhost for server binding.
     *
     * @return a free port number
     * @throws Exception if unable to find a free port
     */
    private static int freePort() throws Exception {
        try (var s = new ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        }
    }

    /**
     * Creates a JWT bearer token for authentication.
     *
     * @return metadata with Authorization header containing JWT bearer token
     */
    private static Metadata bearerMeta() {
        SecretKey key = Validator.fromBase64String(SECRET_KEY_BASE64);
        Calendar exp = Calendar.getInstance();
        exp.add(Calendar.HOUR, 1);
        String token = Jwts.builder().subject("admin").expiration(exp.getTime()).signWith(key).compact();
        var meta = new Metadata();
        meta.put(AUTHORIZATION_KEY, "Bearer " + token);
        return meta;
    }
}
