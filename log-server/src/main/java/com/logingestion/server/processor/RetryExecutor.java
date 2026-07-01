package com.logingestion.server.processor;

import com.logingestion.server.config.ServerProperties;
import com.logingestion.server.stats.ServerStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/**
 * Thực thi một operation với exponential backoff retry.
 *
 * Retry policy (configurable):
 *   attempt 1 : execute immediately
 *   attempt 2 : wait initialBackoffMs (100ms)
 *   attempt 3 : wait initialBackoffMs * 2 (200ms)
 *   attempt 4 : wait min(400ms, maxBackoffMs)
 *   ...max retries = 3 (total 4 attempts)
 */
public class RetryExecutor {

    private static final Logger log = LoggerFactory.getLogger(RetryExecutor.class);

    private final int         maxRetries;
    private final long        initialBackoffMs;
    private final long        maxBackoffMs;
    private final ServerStats stats;

    public RetryExecutor(ServerProperties props, ServerStats stats) {
        this.maxRetries       = props.getRetry().getMaxRetries();
        this.initialBackoffMs = props.getRetry().getInitialBackoffMs();
        this.maxBackoffMs     = props.getRetry().getMaxBackoffMs();
        this.stats            = stats;
    }

    /**
     * Thực thi callable với retry policy.
     *
     * @param action  Operation cần thực thi (và retry nếu lỗi)
     * @param ctx     Context label cho logging
     * @return true nếu thành công trong giới hạn retry
     */
    public boolean executeWithRetry(Callable<Boolean> action, String ctx) {
        long backoff  = initialBackoffMs;
        Exception last = null;

        for (int attempt = 1; attempt <= maxRetries + 1; attempt++) {
            try {
                if (Boolean.TRUE.equals(action.call())) {
                    if (attempt > 1) {
                        log.info("[{}] succeeded on attempt {}/{}", ctx, attempt, maxRetries + 1);
                    }
                    return true;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            } catch (Exception e) {
                last = e;
                log.warn("[{}] attempt {}/{} failed: {}", ctx, attempt, maxRetries + 1, e.getMessage());
            }

            if (attempt <= maxRetries) {
                stats.recordRetryAttempt();
                log.debug("[{}] retrying in {}ms...", ctx, backoff);
                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return false;
                }
                // Exponential backoff với cap
                backoff = Math.min(backoff * 2, maxBackoffMs);
            }
        }

        stats.recordRetriesExhausted();
        log.error("[{}] all {} retries exhausted. Last error: {}",
            ctx, maxRetries, last != null ? last.getMessage() : "unknown");
        return false;
    }
}
