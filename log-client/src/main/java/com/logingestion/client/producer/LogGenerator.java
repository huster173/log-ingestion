package com.logingestion.client.producer;

import com.logingestion.client.config.ClientConfig;
import com.logingestion.client.sender.LogSender;
import com.logingestion.client.stats.ClientStats;
import com.logingestion.common.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Log generator: sinh log entries theo TPS target, gom thành
 * client-side batch rồi gửi qua LogSender.
 *
 * Mỗi producer thread:
 *   1. Sinh log với token-bucket rate limiting
 *   2. Gom vào local buffer
 *   3. Flush khi đủ clientBatchSize HOẶC đủ clientFlushMs
 *   4. Gọi LogSender.send() → HTTP POST /logs/batch
 *
 * Điều này tối ưu hoá HTTP overhead: thay vì N request/s,
 * chỉ cần N/batchSize request/s.
 */
public class LogGenerator {

    private static final Logger log = LoggerFactory.getLogger(LogGenerator.class);

    private static final String[] METHODS = {
        "GET","GET","GET","GET","POST","POST","PUT","DELETE"
    };
    private static final String[] PATHS = {
        "/api/users","/api/products","/api/orders","/api/search",
        "/api/cart","/api/checkout","/api/auth/login","/api/auth/logout",
        "/api/feed","/api/notifications","/api/upload","/health",
        "/api/payments","/api/recommendations","/static/js/app.js"
    };
    private static final int[] STATUS_CODES = {
        200,200,200,200,200,201,201,204,301,304,400,401,403,404,404,500,503
    };

    private final ClientConfig    config;
    private final LogSender       sender;
    private final ClientStats     stats;
    private final ExecutorService executor;
    private final AtomicBoolean   running = new AtomicBoolean(false);

    public LogGenerator(ClientConfig config, LogSender sender, ClientStats stats) {
        this.config   = config;
        this.sender   = sender;
        this.stats    = stats;
        this.executor = Executors.newFixedThreadPool(config.getProducerThreads(),
            r -> { Thread t = new Thread(r, "log-gen-" + System.nanoTime()); t.setDaemon(true); return t; });
    }

    public void start() {
        if (!running.compareAndSet(false, true)) return;
        int threads    = config.getProducerThreads();
        int tpsPerThread = Math.max(1, config.getTargetTps() / threads);
        log.info("Starting {} generator threads @ {} TPS each = {} TPS total",
            threads, tpsPerThread, tpsPerThread * threads);
        for (int i = 0; i < threads; i++) {
            final int tps = tpsPerThread;
            executor.submit(() -> generatorLoop(tps));
        }
    }

    private void generatorLoop(int targetTps) {
        Random rng            = new Random();
        long   intervalNs     = 1_000_000_000L / targetTps;
        long   nextFireNs     = System.nanoTime();
        long   lastFlushMs    = System.currentTimeMillis();

        List<LogEntry> buffer = new ArrayList<>(config.getClientBatchSize());

        while (running.get()) {
            long now = System.nanoTime();

            if (now >= nextFireNs) {
                // Generate one log entry
                LogEntry entry = new LogEntry(
                    Instant.now(),
                    generateIp(rng),
                    METHODS[rng.nextInt(METHODS.length)],
                    PATHS[rng.nextInt(PATHS.length)],
                    STATUS_CODES[rng.nextInt(STATUS_CODES.length)]
                );
                buffer.add(entry);
                stats.recordGenerated(1);

                nextFireNs += intervalNs;
                if (nextFireNs < now - intervalNs * 10) nextFireNs = now + intervalNs;

                // Flush condition: batch full
                if (buffer.size() >= config.getClientBatchSize()) {
                    flush(buffer);
                    buffer = new ArrayList<>(config.getClientBatchSize());
                    lastFlushMs = System.currentTimeMillis();
                }
            } else {
                // Check time-based flush
                long elapsedMs = System.currentTimeMillis() - lastFlushMs;
                if (!buffer.isEmpty() && elapsedMs >= config.getClientFlushMs()) {
                    flush(buffer);
                    buffer = new ArrayList<>(config.getClientBatchSize());
                    lastFlushMs = System.currentTimeMillis();
                }

                // Sleep to avoid busy-spin
                long sleepNs = Math.min(nextFireNs - now, config.getClientFlushMs() * 500_000L);
                if (sleepNs > 100_000) {
                    try { Thread.sleep(sleepNs / 1_000_000, (int)(sleepNs % 1_000_000)); }
                    catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
                }
            }
        }

        // Final flush on shutdown
        if (!buffer.isEmpty()) flush(buffer);
    }

    private void flush(List<LogEntry> buffer) {
        if (!buffer.isEmpty()) {
            sender.send(new ArrayList<>(buffer));
        }
    }

    private String generateIp(Random rng) {
        return (10 + rng.nextInt(20)) + "." +
               rng.nextInt(256) + "." +
               rng.nextInt(256) + "." +
               rng.nextInt(256);
    }

    public void stop() {
        log.info("Stopping LogGenerator...");
        running.set(false);
        executor.shutdown();
        try { executor.awaitTermination(10, TimeUnit.SECONDS); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        log.info("LogGenerator stopped. Total generated: {}", stats.getTotalGenerated());
    }
}
