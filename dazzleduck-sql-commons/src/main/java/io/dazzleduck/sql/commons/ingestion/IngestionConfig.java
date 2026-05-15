package io.dazzleduck.sql.commons.ingestion;

import com.typesafe.config.Config;
import io.dazzleduck.sql.common.ConfigConstants;

import java.time.Duration;

/**
 * Queue tuning parameters for a {@link ParquetIngestionQueue}.
 *
 * <p>Separates operational concerns (flush thresholds, delays) from domain concerns
 * (output path, transformation, partition columns) which are provided by
 * {@link IngestionHandler}.
 */
public record IngestionConfig(long minBucketSize,
                               long maxBucketSize,
                               int  maxBatches,
                               long maxPendingWrite,
                               Duration maxDelay,
                               Duration configRefreshDelay) {

    public static final long     DEFAULT_MAX_BUCKET_SIZE   = 100L * 1024 * 1024; // 100 MB
    public static final long     DEFAULT_MAX_PENDING_WRITE = 500L * 1024 * 1024; // 500 MB
    public static final int      DEFAULT_MAX_BATCHES       = Integer.MAX_VALUE;
    public static final Duration DEFAULT_CONFIG_REFRESH    = Duration.ofMinutes(2);

    public static IngestionConfig fromConfig(Config config) {
        return new IngestionConfig(
                config.getLong(ConfigConstants.MIN_BUCKET_SIZE_KEY),
                config.hasPath(ConfigConstants.MAX_BUCKET_SIZE_KEY)
                        ? config.getLong(ConfigConstants.MAX_BUCKET_SIZE_KEY) : DEFAULT_MAX_BUCKET_SIZE,
                config.hasPath(ConfigConstants.MAX_BATCHES_KEY)
                        ? config.getInt(ConfigConstants.MAX_BATCHES_KEY)      : DEFAULT_MAX_BATCHES,
                config.hasPath(ConfigConstants.MAX_PENDING_WRITE_KEY)
                        ? config.getLong(ConfigConstants.MAX_PENDING_WRITE_KEY) : DEFAULT_MAX_PENDING_WRITE,
                Duration.ofMillis(config.getLong(ConfigConstants.MAX_DELAY_MS_KEY)),
                config.hasPath(ConfigConstants.QUEUE_CONFIG_REFRESH_DELAY_MS_KEY)
                        ? Duration.ofMillis(config.getLong(ConfigConstants.QUEUE_CONFIG_REFRESH_DELAY_MS_KEY))
                        : DEFAULT_CONFIG_REFRESH);
    }
}
