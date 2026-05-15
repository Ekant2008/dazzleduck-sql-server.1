package io.dazzleduck.sql.otel.collector;

import io.dazzleduck.sql.commons.ingestion.Batch;
import io.dazzleduck.sql.commons.ingestion.IngestionConfig;
import io.dazzleduck.sql.commons.ingestion.IngestionHandler;
import io.dazzleduck.sql.commons.ingestion.ParquetIngestionQueue;
import io.dazzleduck.sql.commons.ingestion.Stats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Accepts Arrow batches from OTLP services, spills each batch to a temp Arrow
 * file on disk, and delegates batching + Parquet writing to {@link ParquetIngestionQueue}.
 *
 * <p>The queue is created once via {@link IngestionHandler#getOrCreateQueue}. State refresh
 * (output path, transformation, partition columns) is handled lazily inside the handler —
 * {@link #addBatch} calls {@code getOrCreateQueue} on every batch so the handler's stale-state
 * check runs automatically without a separate scheduler.
 *
 * <p>Queue tuning parameters (bucket sizes, flush delay) come from {@link IngestionConfig},
 * following the same pattern as the flight module.
 */
public class SignalWriter implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(SignalWriter.class);

    private final String queueId;
    private final ScheduledExecutorService scheduler;
    private final IngestionHandler ingestionHandler;
    private final IngestionHandler.QueueCreator creator;
    private final IngestionHandler.QueueEventListener logListener;
    /** Cached for {@link #getStats()} and {@link #close()}. */
    private volatile ParquetIngestionQueue queue;

    public SignalWriter(String queueId,
                        IngestionHandler ingestionHandler,
                        IngestionConfig ingestionConfig) throws IOException {
        this.queueId          = queueId;
        this.ingestionHandler = ingestionHandler;

        String outputPath = ingestionHandler.getTargetPath(queueId);
        if (outputPath == null) outputPath = "./" + queueId;
        Files.createDirectories(Path.of(outputPath));
        final String resolvedOutputPath = outputPath;

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "otel-signal-scheduler[" + queueId + "]");
            t.setDaemon(true);
            return t;
        });

        // _targetPath from the handler is the DuckLake table path (used for tombstone detection).
        // resolvedOutputPath is where Arrow→Parquet files are written; it is stable for the
        // lifetime of this writer. If the table path ever moves, tombstone detection in
        // getOrCreateQueue() will surface it — the writer would need to be recreated.
        this.creator = (id, _targetPath) -> new ParquetIngestionQueue(
                "otel-collector", "arrow", resolvedOutputPath, id,
                ingestionConfig.minBucketSize(),
                ingestionConfig.maxBucketSize(),
                ingestionConfig.maxBatches(),
                ingestionConfig.maxPendingWrite(),
                ingestionConfig.maxDelay(),
                ingestionHandler,
                scheduler, Clock.systemUTC());

        this.logListener = new IngestionHandler.QueueEventListener() {
            @Override public void onCreated(String id)   { log.info("Queue created: {}", id);   }
            @Override public void onRefreshed(String id) { log.debug("Queue refreshed: {}", id); }
            @Override public void onDeleted(String id)   { log.warn("Queue deleted: {}", id);   }
        };

        this.queue = ingestionHandler.getOrCreateQueue(queueId, creator, logListener);
        if (this.queue == null) {
            log.info("Handler returned no target path for '{}', using local output path: {}", queueId, resolvedOutputPath);
            this.queue = creator.create(queueId, resolvedOutputPath);
        }
    }

    /**
     * Submits an Arrow file to the ingestion queue.
     * Calls {@code getOrCreateQueue} on each batch so the handler's lazy refresh and
     * tombstone detection run automatically.
     */
    public CompletableFuture<Void> addBatch(Path arrowFile) {
        ParquetIngestionQueue q = ingestionHandler.getOrCreateQueue(queueId, creator, logListener);
        if (q == null) q = this.queue;
        else this.queue = q;
        try {
            long fileSize = Files.size(arrowFile);
            Batch<String> batch = new Batch<>(new String[0], new String[0],
                    arrowFile.toString(), null, 0, fileSize, "parquet", Instant.now());
            return q.add(batch).thenApply(ignored -> null);
        } catch (IOException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    public Stats getStats() {
        return queue.getStats();
    }

    @Override
    public void close() {
        try {
            queue.close();
        } catch (Exception e) {
            log.warn("Error closing ingestion queue for '{}'", queueId, e);
        }
        scheduler.shutdown();
        log.info("SignalWriter[{}] closed", queueId);
    }
}
