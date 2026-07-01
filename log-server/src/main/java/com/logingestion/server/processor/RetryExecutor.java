package com.logingestion.server.processor;

import com.logingestion.server.config.ServerConfig;
import com.logingestion.server.stats.ServerStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

public class RetryExecutor {

    private static final Logger log = LoggerFactory.getLogger(RetryExecutor.class);

    private final int     maxRetries;
    private final long    initialBackoffMs;
    private final long    maxBackoffMs;
    private final ServerStats stats;

    public RetryExecutor(ServerConfig config, ServerStats stats) {
        this.maxRetries       = config.getMaxRetries();
        this.initialBackoffMs = config.getInitialBackoffMs();
        this.maxBackoffMs     = config.getMaxBackoffMs();
        this.stats            = stats;
    }

    public boolean executeWithRetry(Callable<Boolean> action, String ctx) {
        long backoff = initialBackoffMs;
        Exception last = null;
        for (int attempt = 1; attempt <= maxRetries + 1; attempt++) {
            try {
                if (Boolean.TRUE.equals(action.call())) return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            } catch (Exception e) {
                last = e;
                log.warn("Attempt {}/{} failed [{}]: {}", attempt, maxRetries + 1, ctx, e.getMessage());
            }
            if (attempt <= maxRetries) {
                stats.recordRetryAttempt();
                try { Thread.sleep(backoff); } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); return false;
                }
                backoff = Math.min(backoff * 2, maxBackoffMs);
            }
        }
        stats.recordRetriesExhausted();
        log.error("❌ All retries exhausted [{}]: {}", ctx, last != null ? last.getMessage() : "?");
        return false;
    }
}
