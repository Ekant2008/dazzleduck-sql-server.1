package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.flight.MicroMeterFlightRecorder;
import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.*;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies per-identity and server-wide cursor limits return RESOURCE_EXHAUSTED.
 *
 * Cursors are created by DoGet (getStream), not GetFlightInfo (execute).
 * We use injectTestCursor() to populate the cache directly so the test is not
 * sensitive to gRPC in-process flow-control timing.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CursorLimitTest {

    private static final int PER_IDENTITY_LIMIT = 3;
    private static final int TOTAL_LIMIT        = 5;

    private BufferAllocator allocator;
    private FlightServer server;
    private FlightSqlClient client;
    private DuckDBFlightSqlProducer producer;

    @BeforeAll
    void setup() throws Exception {
        allocator = new RootAllocator(Long.MAX_VALUE);
        ConnectionPool.executeBatch(new String[]{"INSTALL arrow FROM community", "LOAD arrow"});

        var cursorConfig = new CursorConfig(30_000, PER_IDENTITY_LIMIT, TOTAL_LIMIT);

        Location location = FlightTestUtils.findNextLocation();
        producer = new DuckDBFlightSqlProducer(
                location,
                UUID.randomUUID().toString(),
                "test-secret",
                allocator,
                System.getProperty("java.io.tmpdir"),
                AccessMode.COMPLETE,
                DuckDBFlightSqlProducer.newTempDir(),
                null,
                Executors.newSingleThreadScheduledExecutor(),
                Duration.ofMinutes(2),
                Duration.ZERO,
                Clock.systemDefaultZone(),
                new MicroMeterFlightRecorder(new SimpleMeterRegistry(), "test"),
                DuckDBFlightSqlProducer.DEFAULT_INGESTION_CONFIG,
                List.of(),
                cursorConfig
        );

        server = FlightServer.builder(allocator, location, producer)
                .headerAuthenticator(AuthUtils.getTestAuthenticator())
                .build()
                .start();

        client = new FlightSqlClient(FlightClient.builder(allocator, location)
                .intercept(AuthUtils.createClientMiddlewareFactory("admin", "password", Map.of()))
                .build());
    }

    @AfterAll
    void teardown() throws Exception {
        if (client != null) client.close();
        if (server != null) server.close();
        if (allocator != null) allocator.close();
    }

    @AfterEach
    void clearCache() {
        // Drain the cache between tests so limits don't bleed across test methods.
        producer.statementLoadingCache.invalidateAll();
        producer.statementLoadingCache.cleanUp();
    }

    @Test
    void perIdentityLimit_rejectsNextCursorWithResourceExhausted() throws Exception {
        // peerIdentity() returns the authenticated username — "admin" in these tests.
        for (int i = 0; i < PER_IDENTITY_LIMIT; i++) {
            producer.injectTestCursor("admin");
        }

        // The next getStream from "admin" must be rejected — simulate via a real ticket.
        FlightInfo info = client.execute("SELECT 1");
        Ticket ticket = info.getEndpoints().get(0).getTicket();

        FlightRuntimeException ex = assertThrows(FlightRuntimeException.class,
                () -> { try (FlightStream s = client.getStream(ticket)) { s.next(); } });
        assertEquals(FlightStatusCode.RESOURCE_EXHAUSTED, ex.status().code(),
                "Expected RESOURCE_EXHAUSTED when per-identity limit is hit");
        assertTrue(ex.getMessage().contains("Too many open cursors"),
                "Error should mention cursor limit, was: " + ex.getMessage());
    }

    @Test
    void serverWideLimit_rejectsAnyNewCursorWithResourceExhausted() throws Exception {
        // Fill the cache to the server-wide total across different identities.
        for (int i = 0; i < TOTAL_LIMIT; i++) {
            producer.injectTestCursor("user-" + i);
        }

        FlightInfo info = client.execute("SELECT 1");
        Ticket ticket = info.getEndpoints().get(0).getTicket();

        FlightRuntimeException ex = assertThrows(FlightRuntimeException.class,
                () -> { try (FlightStream s = client.getStream(ticket)) { s.next(); } });
        assertEquals(FlightStatusCode.RESOURCE_EXHAUSTED, ex.status().code(),
                "Expected RESOURCE_EXHAUSTED when server-wide limit is hit");
        assertTrue(ex.getMessage().contains("Server cursor limit reached"),
                "Error should mention server limit, was: " + ex.getMessage());
    }

    @Test
    void underLimit_querySucceeds() throws Exception {
        // One less than the limit — must succeed.
        for (int i = 0; i < PER_IDENTITY_LIMIT - 1; i++) {
            producer.injectTestCursor("bob");
        }

        FlightInfo info = client.execute("SELECT 42 AS answer");
        try (FlightStream stream = client.getStream(info.getEndpoints().get(0).getTicket())) {
            assertTrue(stream.next(), "Expected at least one batch");
        }
    }
}
