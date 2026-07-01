package com.logingestion.server.stats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Thread-safe server statistics với auto-scheduled reporting.
 *
 * Dùng LongAdder thay AtomicLong để tối ưu contention khi nhiều thread ghi đồng thời.
 * @Scheduled tự động in report theo interval cấu hình trong application.yml.
 */
@Component
public class ServerStats {

    private static final Logger log = LoggerFactory.getLogger(ServerStats.class);

    // ── HTTP layer ──────────────────────────────────────────────────────────
    private final LongAdder httpRequests     = new LongAdder();
    private final LongAdder logsReceived     = new LongAdder();
    private final LongAdder logsDropped      = new LongAdder();
    private final LongAdder httpErrors       = new LongAdder();

    // ── Queue ───────────────────────────────────────────────────────────────
    private final AtomicLong queueDepth      = new AtomicLong();
    private final AtomicLong queuePeak       = new AtomicLong();

    // ── Batch processing ────────────────────────────────────────────────────
    private final LongAdder logsInserted     = new LongAdder();
    private final LongAdder batchesWritten   = new LongAdder();
    private final LongAdder batchesFailed    = new LongAdder();
    private final LongAdder retryAttempts    = new LongAdder();
    private final LongAdder retriesExhausted = new LongAdder();

    // ── TPS tracking (updated mỗi report cycle) ─────────────────────────────
    private volatile long   lastReceived = 0;
    private volatile long   lastInserted = 0;
    private volatile long   lastReportMs = System.currentTimeMillis();
    private volatile double receiveTps   = 0.0;
    private volatile double insertTps    = 0.0;

    // ── Record methods ──────────────────────────────────────────────────────
    public void recordHttpRequest()      { httpRequests.increment(); }
    public void recordReceived(int n)    { logsReceived.add(n); }
    public void recordDropped(int n)     { logsDropped.add(n); }
    public void recordHttpError()        { httpErrors.increment(); }
    public void recordInserted(int n)    { logsInserted.add(n); batchesWritten.increment(); }
    public void recordBatchFailed()      { batchesFailed.increment(); }
    public void recordRetryAttempt()     { retryAttempts.increment(); }
    public void recordRetriesExhausted() { retriesExhausted.increment(); }

    public void updateQueueDepth(long d) {
        queueDepth.set(d);
        queuePeak.updateAndGet(p -> Math.max(p, d));
    }

    // ── Getters cho StatsController ─────────────────────────────────────────
    public long   getTotalReceived()    { return logsReceived.sum(); }
    public long   getTotalInserted()    { return logsInserted.sum(); }
    public long   getTotalDropped()     { return logsDropped.sum(); }
    public long   getBatchesWritten()   { return batchesWritten.sum(); }
    public long   getBatchesFailed()    { return batchesFailed.sum(); }
    public long   getRetryAttempts()    { return retryAttempts.sum(); }
    public long   getRetriesExhausted() { return retriesExhausted.sum(); }
    public long   getQueueDepth()       { return queueDepth.get(); }
    public long   getQueuePeak()        { return queuePeak.get(); }
    public double getReceiveTps()       { return receiveTps; }
    public double getInsertTps()        { return insertTps; }
    public long   getHttpRequests()     { return httpRequests.sum(); }
    public long   getHttpErrors()       { return httpErrors.sum(); }

    // ── Scheduled auto-report ────────────────────────────────────────────────
    /**
     * Tự động in stats report theo interval.
     * fixedDelay đảm bảo không chồng chéo nếu report chạy lâu.
     */
    @Scheduled(fixedDelayString = "${log.server.stats.interval-ms:5000}")
    public void report() {
        long now     = System.currentTimeMillis();
        long elapsed = Math.max(1, now - lastReportMs);
        long curRecv = logsReceived.sum();
        long curIns  = logsInserted.sum();

        // Tính TPS trong khoảng interval vừa rồi
        receiveTps = (curRecv - lastReceived) * 1000.0 / elapsed;
        insertTps  = (curIns  - lastInserted) * 1000.0 / elapsed;

        lastReceived = curRecv;
        lastInserted = curIns;
        lastReportMs = now;

        long avgBatch = batchesWritten.sum() > 0
            ? logsInserted.sum() / batchesWritten.sum() : 0;

        // JVM memory stats
        MemoryMXBean mem      = ManagementFactory.getMemoryMXBean();
        long         heapUsed = mem.getHeapMemoryUsage().getUsed() / 1_048_576;
        long         heapMax  = mem.getHeapMemoryUsage().getMax()  / 1_048_576;

        // CPU stats (sun.management extension)
        String cpuStr = "N/A";
        java.lang.management.OperatingSystemMXBean os =
            ManagementFactory.getOperatingSystemMXBean();
        if (os instanceof com.sun.management.OperatingSystemMXBean sunOs) {
            cpuStr = String.format("%.1f%%", sunOs.getProcessCpuLoad() * 100);
        }

        log.info(String.format(
            "%n╔══════════════════════════════════════════════════════════╗" +
            "%n║                  SERVER STATS REPORT                    ║" +
            "%n╠══════════════════════════════════════════════════════════╣" +
            "%n║  THROUGHPUT                                              ║" +
            "%n║    Receive TPS    : %8.0f logs/s                    ║" +
            "%n║    Insert  TPS    : %8.0f logs/s                    ║" +
            "%n╠══════════════════════════════════════════════════════════╣" +
            "%n║  HTTP                                                    ║" +
            "%n║    Requests       : %,12d                         ║" +
            "%n║    Errors         : %,12d                         ║" +
            "%n╠══════════════════════════════════════════════════════════╣" +
            "%n║  PIPELINE                                                ║" +
            "%n║    Received       : %,12d                         ║" +
            "%n║    Inserted DB    : %,12d                         ║" +
            "%n║    Dropped        : %,12d                         ║" +
            "%n╠══════════════════════════════════════════════════════════╣" +
            "%n║  QUEUE                                                   ║" +
            "%n║    Depth now      : %,12d                         ║" +
            "%n║    Peak depth     : %,12d                         ║" +
            "%n╠══════════════════════════════════════════════════════════╣" +
            "%n║  BATCHES & RETRIES                                       ║" +
            "%n║    Batches written: %,12d  avg=%,6d           ║" +
            "%n║    Batches failed : %,12d                         ║" +
            "%n║    Retry attempts : %,12d                         ║" +
            "%n║    Retries exh.   : %,12d                         ║" +
            "%n╠══════════════════════════════════════════════════════════╣" +
            "%n║  JVM / SYSTEM                                            ║" +
            "%n║    Heap           : %4d MB / %4d MB                  ║" +
            "%n║    CPU (process)  : %-10s                           ║" +
            "%n╚══════════════════════════════════════════════════════════╝",
            receiveTps, insertTps,
            httpRequests.sum(), httpErrors.sum(),
            curRecv, curIns, logsDropped.sum(),
            queueDepth.get(), queuePeak.get(),
            batchesWritten.sum(), avgBatch,
            batchesFailed.sum(), retryAttempts.sum(), retriesExhausted.sum(),
            heapUsed, heapMax, cpuStr));
    }
}
