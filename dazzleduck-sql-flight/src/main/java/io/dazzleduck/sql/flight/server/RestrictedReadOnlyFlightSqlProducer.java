package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.commons.ingestion.IngestionHandler;
import io.dazzleduck.sql.flight.FlightRecorder;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Flight SQL producer for RESTRICT_READ_ONLY mode.
 *
 * Inherits SELECT-only query transformation (EXPLAIN handling, AST parse/authorize/serialize)
 * from {@link SelectOnlyFlightSqlProducer} and additionally blocks all prepared-statement
 * and schema-probe entry points that bypass {@code transformQuery}.
 */
public class RestrictedReadOnlyFlightSqlProducer extends SelectOnlyFlightSqlProducer {

    public RestrictedReadOnlyFlightSqlProducer(
            Location serverLocation, String producerId, String secretKey,
            BufferAllocator allocator, String warehousePath, AccessMode accessMode,
            Path tempDir, IngestionHandler postIngestionHandler,
            ScheduledExecutorService scheduledExecutorService,
            Duration queryTimeout, Duration maxQueryTimeout,
            Clock clock, FlightRecorder recorder,
            IngestionConfig ingestionConfig, List<Location> dataProcessorLocations) {
        super(serverLocation, producerId, secretKey, allocator, warehousePath, accessMode,
              tempDir, postIngestionHandler, scheduledExecutorService,
              queryTimeout, maxQueryTimeout, clock, recorder, ingestionConfig, dataProcessorLocations);
    }

    // ── Block all prepared-statement and schema-probe entry points ───────────

    @Override
    public void createPreparedStatement(
            FlightSql.ActionCreatePreparedStatementRequest request,
            CallContext context, StreamListener<Result> listener) {
        throwNotSupported("createPreparedStatement");
    }

    @Override
    public final FlightInfo getFlightInfoPreparedStatement(
            FlightSql.CommandPreparedStatementQuery command,
            CallContext context, FlightDescriptor descriptor) {
        return throwNotSupported("getFlightInfoPreparedStatement");
    }

    @Override
    public final void getStreamPreparedStatement(
            FlightSql.CommandPreparedStatementQuery command,
            CallContext context, ServerStreamListener listener) {
        throwNotSupported("getStreamPreparedStatement");
    }

    @Override
    public final SchemaResult getSchemaPreparedStatement(
            FlightSql.CommandPreparedStatementQuery command,
            CallContext context, FlightDescriptor descriptor) {
        return throwNotSupported("getSchemaPreparedStatement");
    }

    @Override
    public final Runnable acceptPutPreparedStatementUpdate(
            FlightSql.CommandPreparedStatementUpdate command,
            CallContext context, FlightStream flightStream,
            StreamListener<PutResult> ackStream) {
        return throwNotSupported("acceptPutPreparedStatementUpdate");
    }

    @Override
    public final Runnable acceptPutPreparedStatementQuery(
            FlightSql.CommandPreparedStatementQuery command,
            CallContext context, FlightStream flightStream,
            StreamListener<PutResult> ackStream) {
        return throwNotSupported("acceptPutPreparedStatementQuery");
    }

    @Override
    public final SchemaResult getSchemaStatement(
            FlightSql.CommandStatementQuery command,
            CallContext context, FlightDescriptor descriptor) {
        return throwNotSupported("getSchemaStatement");
    }
}
