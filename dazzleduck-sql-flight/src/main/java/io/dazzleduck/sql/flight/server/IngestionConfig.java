package io.dazzleduck.sql.flight.server;

import com.typesafe.config.Config;

import java.time.Duration;

/**
 * @deprecated Use {@link io.dazzleduck.sql.commons.ingestion.IngestionConfig}.
 *             <p>Source-compatible: existing call sites compile unchanged.
 *             <p><b>Binary-incompatible</b>: this was previously a {@code record}; it is now a
 *             {@code final class}. Pre-compiled artifacts that pattern-match on it as a record
 *             or use its component accessors reflectively must be recompiled.
 */
@Deprecated
public final class IngestionConfig {

    private final io.dazzleduck.sql.commons.ingestion.IngestionConfig delegate;

    public IngestionConfig(long minBucketSize, long maxBucketSize, int maxBatches,
                           long maxPendingWrite, Duration maxDelay, Duration configRefreshDelay) {
        this.delegate = new io.dazzleduck.sql.commons.ingestion.IngestionConfig(
                minBucketSize, maxBucketSize, maxBatches, maxPendingWrite, maxDelay, configRefreshDelay);
    }

    public long     minBucketSize()    { return delegate.minBucketSize(); }
    public long     maxBucketSize()    { return delegate.maxBucketSize(); }
    public int      maxBatches()       { return delegate.maxBatches(); }
    public long     maxPendingWrite()  { return delegate.maxPendingWrite(); }
    public Duration maxDelay()         { return delegate.maxDelay(); }
    public Duration configRefreshDelay(){ return delegate.configRefreshDelay(); }

    public static IngestionConfig fromConfig(Config config) {
        var b = io.dazzleduck.sql.commons.ingestion.IngestionConfig.fromConfig(config);
        return new IngestionConfig(b.minBucketSize(), b.maxBucketSize(), b.maxBatches(),
                b.maxPendingWrite(), b.maxDelay(), b.configRefreshDelay());
    }

    /** Converts to the canonical commons type. */
    public io.dazzleduck.sql.commons.ingestion.IngestionConfig toCommonsConfig() {
        return delegate;
    }
}
