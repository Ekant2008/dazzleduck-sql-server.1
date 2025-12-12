package io.dazzleduck.sql.logger;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.client.ArrowHttpPoster;
import io.dazzleduck.sql.common.ingestion.FlightSender;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.UUID;

/**
 * FlightSender implementation that uses ArrowHttpPoster:
 * - Background queue processing thread (inherited from AbstractFlightSender)
 * - Appends UUID to each request URL
 * - Sends logs immediately as they are dequeued
 */
public class HttpFlightSender extends FlightSender.AbstractFlightSender {

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final Duration timeout = Duration.ofSeconds(30);

    public HttpFlightSender() {
        loadEndpointFromConfig();
    }

    /**
     * Load endpoint from configuration file
     */
    private void loadEndpointFromConfig() {
        try {
            Config config = ConfigFactory.load().getConfig("dazzleduck_logger");
            String endpoint = config.getString("http_endpoint");
            setIngestEndpoint(endpoint);
        } catch (Exception e) {
            System.err.println("[HttpFlightSender] Failed to load endpoint from config: " + e.getMessage());
        }
    }

    @Override
    public long getMaxInMemorySize() {
        return 100 * 1024 * 1024; // 100 MB
    }

    @Override
    public long getMaxOnDiskSize() {
        return 1024 * 1024 * 1024; // 1 GB
    }

    /**
     * Start the main processing thread only
     * No periodic flush needed - the queue processing thread handles everything
     */
    @Override
    public void start() {
        super.start(); // Start the main queue processing thread

        // NOTE: We don't need the periodic flush scheduler anymore
        // The AbstractFlightSender's queue processing thread already
        // processes items immediately as they arrive via doSend()
    }

    @Override
    protected void doSend(SendElement element) throws InterruptedException {
        try {
            byte[] data = element.read().readAllBytes();

            // Append UUID to URL like old ArrowHttpPoster
            String uuid = UUID.randomUUID().toString();
            String finalUrl = ingestEndpoint  + uuid;

            int status = ArrowHttpPoster.postBytes(
                    httpClient,
                    data,
                    finalUrl,
                    timeout
            );

            if (status / 100 != 2) {
                System.err.println("[HttpFlightSender] POST to " + finalUrl +
                        " failed with status: " + status);
            }
        } catch (Exception e) {
            System.err.println("[HttpFlightSender] Send failed: " + e.getMessage());
            e.printStackTrace();
        } finally {
            element.cleanup();
        }
    }

    /**
     * Shutdown the sender and cleanup resources
     */
    public void close() {
        // No scheduler to shutdown anymore
        // The AbstractFlightSender daemon thread will stop when JVM exits
    }
}