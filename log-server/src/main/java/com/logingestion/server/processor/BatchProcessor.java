package com.logingestion.server.processor;

import com.logingestion.common.LogEntry;
import com.logingestion.server.config.ServerProperties;
import com.logingestion.server.queue.LogQueue;
import com.logingestion.server.repository.LogRepository;
import com.logingestion.server.stats.ServerStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Worker pool drain LogQueue → batch insert vào DB.
 *
 * N worker threads chạy song song, mỗi thread:
 *   1. Drain batch từ queue (chờ tối đa flushMs nếu queue rỗng)
 *   2. JDBC batch insert với retry + exponential backoff
 *   3. Cập nhật ServerStats
 *
 * Lifecycle quản lý bởi Spring:
 *   - start() gọi qua @Bean(initMethod = "start")
 *   - stop()  gọi qua @Bean(destroyMethod = "stop") — đảm bảo drain hết queue
 */
public class BatchProcessor {

    private static final Logger log = LoggerFactory.getLogger(BatchProcessor.class);

    private final ServerProperties props;
    private final LogQueue         queue;
    private final LogRepository    repository;
    private final ServerStats      stats;
    private final RetryExecutor    retry;
    private final ExecutorService  executor;
    private final AtomicBoolean    running     = new AtomicBoolean(false);
    private final AtomicInteger    workerCount = new AtomicInteger(0);

    public BatchProcessor(ServerProperties props, LogQueue queue,
                          LogRepository repository, ServerStats stats) {
        this.props      = props;
        this.queue      = queue;
        this.repository = repository;
        this.stats      = stats;
        this.retry      = new RetryExecutor(props, stats);

        int threads = props.getBatch().getProcessorThreads();
        this.executor = Executors.newFixedThreadPool(threads, r -> {
            int id = workerCount.incrementAndGet();
            Thread t = new Thread(r, "batch-worker-" + id);
            t.setDaemon(false); // non-daemon để JVM chờ drain xong
            return t;
        });
    }

    /** Spring initMethod — khởi động worker threads */
    public void start() {
        if (!running.compareAndSet(false, true)) return;
        int threads = props.getBatch().getProcessorThreads();
        log.info("BatchProcessor starting: {} workers, batchSize={}, flushMs={}",
            threads, props.getBatch().getSize(), props.getBatch().getFlushMs());
        for (int i = 0; i < threads; i++) {
            executor.submit(this::workerLoop);
        }
    }

    private void workerLoop() {
        log.debug("Batch worker started: {}", Thread.currentThread().getName());
        while (running.get() || !queue.isEmpty()) {
            try {
                List<LogEntry> batch = queue.drain(
                    props.getBatch().getSize(),
                    props.getBatch().getFlushMs());
                if (!batch.isEmpty()) {
                    writeBatch(batch);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Worker loop error: {}", e.getMessage(), e);
            }
        }
        log.debug("Batch worker stopped: {}", Thread.currentThread().getName());
    }

    private void writeBatch(List<LogEntry> batch) {
        String ctx = "batch[" + batch.size() + "]";
        boolean ok = retry.executeWithRetry(() -> {
            try {
                int inserted = repository.insertBatch(batch);
                stats.recordInserted(inserted);
                return true;
            } catch (Exception e) {
                stats.recordBatchFailed();
                throw e;
            }
        }, ctx);

        if (!ok) {
            log.error("Dropping {} entries — all retries exhausted", batch.size());
            stats.recordDropped(batch.size());
        }
    }

    /** Spring destroyMethod — graceful shutdown, drain hết queue */
    public void stop() {
        log.info("BatchProcessor stopping — draining remaining {} entries...", queue.size());
        running.set(false);
        executor.shutdown();
        try {
            boolean terminated = executor.awaitTermination(30, TimeUnit.SECONDS);
            if (!terminated) {
                log.warn("Workers did not finish in 30s, forcing shutdown");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
        log.info("BatchProcessor stopped. Total inserted: {}", stats.getTotalInserted());
    }
}
