package com.logingestion.server.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Tất cả tham số cấu hình server — bind từ application.yml
 * Prefix: log.server
 *
 * Ví dụ override qua CLI:
 *   java -jar log-server.jar --log.server.batch.size=1000 --log.server.queue.capacity=500000
 */
@ConfigurationProperties(prefix = "log.server")
public class ServerProperties {

    private Queue queue = new Queue();
    private Batch batch = new Batch();
    private Retry retry = new Retry();
    private Stats stats = new Stats();

    // ── Queue configuration ─────────────────────────────────────────────────
    public static class Queue {
        /** Số lượng log tối đa trong internal queue */
        private int    capacity      = 200_000;
        /** Cảnh báo khi queue đạt X% capacity */
        private double warnThreshold = 0.75;
        /** Bắt đầu drop khi queue đạt X% capacity */
        private double dropThreshold = 0.95;

        public int    getCapacity()             { return capacity; }
        public void   setCapacity(int v)        { capacity = v; }
        public double getWarnThreshold()        { return warnThreshold; }
        public void   setWarnThreshold(double v){ warnThreshold = v; }
        public double getDropThreshold()        { return dropThreshold; }
        public void   setDropThreshold(double v){ dropThreshold = v; }
    }

    // ── Batch processing configuration ─────────────────────────────────────
    public static class Batch {
        /** Số log tối đa trong 1 JDBC batch insert */
        private int  size             = 500;
        /** Flush batch sau X ms dù chưa đủ size */
        private long flushMs          = 200;
        /** Số worker thread xử lý batch song song */
        private int  processorThreads = 4;

        public int  getSize()                  { return size; }
        public void setSize(int v)             { size = v; }
        public long getFlushMs()               { return flushMs; }
        public void setFlushMs(long v)         { flushMs = v; }
        public int  getProcessorThreads()      { return processorThreads; }
        public void setProcessorThreads(int v) { processorThreads = v; }
    }

    // ── Retry configuration ────────────────────────────────────────────────
    public static class Retry {
        /** Số lần retry tối đa khi DB insert lỗi */
        private int  maxRetries       = 3;
        /** Thời gian chờ ban đầu trước khi retry (ms) */
        private long initialBackoffMs = 100;
        /** Thời gian chờ tối đa giữa các retry (ms) */
        private long maxBackoffMs     = 2_000;

        public int  getMaxRetries()             { return maxRetries; }
        public void setMaxRetries(int v)        { maxRetries = v; }
        public long getInitialBackoffMs()       { return initialBackoffMs; }
        public void setInitialBackoffMs(long v) { initialBackoffMs = v; }
        public long getMaxBackoffMs()           { return maxBackoffMs; }
        public void setMaxBackoffMs(long v)     { maxBackoffMs = v; }
    }

    // ── Stats configuration ────────────────────────────────────────────────
    public static class Stats {
        /** Khoảng thời gian giữa các lần in stats (ms) */
        private long intervalMs = 5_000;

        public long getIntervalMs()       { return intervalMs; }
        public void setIntervalMs(long v) { intervalMs = v; }
    }

    public Queue getQueue() { return queue; }
    public void  setQueue(Queue v) { queue = v; }

    public Batch getBatch() { return batch; }
    public void  setBatch(Batch v) { batch = v; }

    public Retry getRetry() { return retry; }
    public void  setRetry(Retry v) { retry = v; }

    public Stats getStats() { return stats; }
    public void  setStats(Stats v) { stats = v; }

    @Override
    public String toString() {
        return String.format(
            "ServerProperties{queue.capacity=%d, batch.size=%d, batch.flushMs=%d, " +
            "batch.threads=%d, retry.max=%d, stats.intervalMs=%d}",
            queue.capacity, batch.size, batch.flushMs,
            batch.processorThreads, retry.maxRetries, stats.intervalMs);
    }
}
