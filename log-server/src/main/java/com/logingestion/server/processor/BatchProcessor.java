package com.logingestion.server.processor;

import com.logingestion.common.LogEntry;
import com.logingestion.server.config.ServerConfig;
import com.logingestion.server.database.LogDatabase;
import com.logingestion.server.queue.LogQueue;
import com.logingestion.server.stats.ServerStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class BatchProcessor {

    private static final Logger log = LoggerFactory.getLogger(BatchProcessor.class);

    private final ServerConfig    config;
    private final LogQueue        queue;
    private final LogDatabase     db;
    private final ServerStats     stats;
    private final RetryExecutor   retry;
    private final ExecutorService executor;
    private final AtomicBoolean   running = new AtomicBoolean(false);

    public BatchProcessor(ServerConfig config, LogQueue queue, LogDatabase db, ServerStats stats) {
        this.config   = config;
        this.queue    = queue;
        this.db       = db;
        this.stats    = stats;
        this.retry    = new RetryExecutor(config, stats);
        this.executor = Executors.newFixedThreadPool(config.getProcessorThreads(),
            r -> { Thread t = new Thread(r, "batch-worker-" + System.nanoTime()); t.setDaemon(false); return t; });
    }

    public void start() {
        if (!running.compareAndSet(false, true)) return;
        log.info("Starting {} batch workers (size={}, flush={}ms)",
            config.getProcessorThreads(), config.getBatchSize(), config.getBatchFlushMs());
        for (int i = 0; i < config.getProcessorThreads(); i++) {
            executor.submit(this::workerLoop);
        }
    }

    private void workerLoop() {
        while (running.get() || !queue.isEmpty()) {
            try {
                List<LogEntry> batch = queue.drain(config.getBatchSize(), config.getBatchFlushMs());
                if (!batch.isEmpty()) writeBatch(batch);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Worker error: {}", e.getMessage(), e);
            }
        }
    }

    private void writeBatch(List<LogEntry> batch) {
        String ctx = "batch[" + batch.size() + "]";
        boolean ok = retry.executeWithRetry(() -> {
            try {
                stats.recordInserted(db.insertBatch(batch));
                return true;
            } catch (SQLException e) {
                stats.recordBatchFailed();
                throw e;
            }
        }, ctx);
        if (!ok) {
            log.error("💀 Dropping {} entries after exhausting retries", batch.size());
        }
    }

    public void stop() {
        log.info("Stopping BatchProcessor, draining queue...");
        running.set(false);
        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS))
                executor.shutdownNow();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }
}
