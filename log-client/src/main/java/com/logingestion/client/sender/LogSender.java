package com.logingestion.client.sender;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.logingestion.client.config.ClientConfig;
import com.logingestion.client.stats.ClientStats;
import com.logingestion.common.LogBatch;
import com.logingestion.common.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

/**
 * Sends a batch of log entries to the server via HTTP POST /logs/batch.
 *
 * Uses JDK 11+ HttpClient (built-in, zero dependency).
 * Connection pool is managed internally by HttpClient.
 *
 * Retry policy: on 5xx or network error, retry up to maxSendRetries
 * with exponential backoff.
 */
public class LogSender {

    private static final Logger log = LoggerFactory.getLogger(LogSender.class);

    private final ClientConfig config;
    private final ClientStats  stats;
    private final ObjectMapper mapper;
    private final HttpClient   httpClient;
    private final URI          batchUri;

    public LogSender(ClientConfig config, ClientStats stats) {
        this.config  = config;
        this.stats   = stats;
        this.mapper  = new ObjectMapper().registerModule(new JavaTimeModule());
        this.batchUri = URI.create(config.getServerUrl() + "/logs/batch");

        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(config.getConnectTimeoutMs()))
            // JDK HttpClient manages a connection pool internally
            .build();
    }

    /**
     * Send a batch synchronously. Called from producer threads.
     * Returns number of logs successfully accepted by server.
     */
    public int send(List<LogEntry> entries) {
        if (entries == null || entries.isEmpty()) return 0;

        byte[] body;
        try {
            body = mapper.writeValueAsBytes(new LogBatch(entries));
        } catch (Exception e) {
            log.error("Serialisation failed: {}", e.getMessage());
            return 0;
        }

        long    backoffMs = config.getSendBackoffMs();
        int     maxRetry  = config.getMaxSendRetries();

        for (int attempt = 1; attempt <= maxRetry + 1; attempt++) {
            try {
                HttpRequest req = HttpRequest.newBuilder()
                    .uri(batchUri)
                    .timeout(Duration.ofMillis(config.getReadTimeoutMs()))
                    .header("Content-Type", "application/json")
                    .header("X-Client-Batch-Size", String.valueOf(entries.size()))
                    .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                    .build();

                stats.recordHttpRequest();
                stats.recordBytesSent(body.length);

                HttpResponse<String> resp = httpClient.send(req,
                    HttpResponse.BodyHandlers.ofString());

                int statusCode = resp.statusCode();

                if (statusCode == 202) {
                    // All accepted
                    stats.recordSent(entries.size());
                    return entries.size();
                } else if (statusCode == 206) {
                    // Partial — parse accepted count
                    int accepted = parseAccepted(resp.body(), entries.size());
                    int dropped  = entries.size() - accepted;
                    stats.recordSent(accepted);
                    stats.recordDropped(dropped);
                    log.debug("Partial acceptance: {}/{}", accepted, entries.size());
                    return accepted;
                } else if (statusCode == 503) {
                    // Server overloaded — retry
                    log.warn("Server overloaded (503) attempt {}/{}, retrying in {}ms",
                        attempt, maxRetry + 1, backoffMs);
                    stats.recordHttpRetry();
                } else if (statusCode == 400) {
                    // Bad request — don't retry
                    log.error("Bad request (400): {}", resp.body());
                    stats.recordHttpError();
                    return 0;
                } else {
                    log.warn("Unexpected status {} attempt {}/{}", statusCode, attempt, maxRetry + 1);
                    stats.recordHttpRetry();
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return 0;
            } catch (Exception e) {
                log.warn("HTTP error attempt {}/{}: {}", attempt, maxRetry + 1, e.getMessage());
                stats.recordHttpError();
                stats.recordHttpRetry();
            }

            // Backoff before retry
            if (attempt <= maxRetry) {
                try { Thread.sleep(backoffMs); } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); return 0;
                }
                backoffMs = Math.min(backoffMs * 2, 5_000);
            }
        }

        // Gave up
        log.error("Failed to send batch of {} entries after {} retries", entries.size(), maxRetry);
        stats.recordDropped(entries.size());
        return 0;
    }

    private int parseAccepted(String body, int fallback) {
        // body: {"received":100,"accepted":85,"dropped":15}
        try {
            var node = mapper.readTree(body);
            return node.has("accepted") ? node.get("accepted").asInt() : fallback;
        } catch (Exception e) {
            return fallback;
        }
    }
}
