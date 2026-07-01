package com.logingestion.client.stats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

/**
 * Client-side statistics với JVM/System metrics.
 * In report định kỳ để dễ quan sát performance khi đang chạy.
 */
public class ClientStats {

    private static final Logger log = LoggerFactory.getLogger(ClientStats.class);

    // ── Log generation ──────────────────────────────────────────────────────
    private final LongAdder logsGenerated = new LongAdder();
    private final LongAdder logsSent      = new LongAdder();
    private final LongAdder logsDropped   = new LongAdder();

    // ── HTTP ────────────────────────────────────────────────────────────────
    private final LongAdder httpRequests  = new LongAdder();
    private final LongAdder httpErrors    = new LongAdder();
    private final LongAdder httpRetries   = new LongAdder();
    private final LongAdder bytesSent     = new LongAdder();

    // ── TPS tracking ────────────────────────────────────────────────────────
    private volatile long lastGenerated = 0;
    private volatile long lastSent      = 0;
    private volatile long lastReportMs  = System.currentTimeMillis();

    // ── Record methods ──────────────────────────────────────────────────────
    public void recordGenerated(int n)   { logsGenerated.add(n); }
    public void recordSent(int n)        { logsSent.add(n); }
    public void recordDropped(int n)     { logsDropped.add(n); }
    public void recordHttpRequest()      { httpRequests.increment(); }
    public void recordHttpError()        { httpErrors.increment(); }
    public void recordHttpRetry()        { httpRetries.increment(); }
    public void recordBytesSent(long b)  { bytesSent.add(b); }

    public long getTotalGenerated() { return logsGenerated.sum(); }
    public long getTotalSent()      { return logsSent.sum(); }

    /** In stats report — gọi theo định kỳ từ scheduled thread */
    public String report() {
        long now     = System.currentTimeMillis();
        long elapsed = Math.max(1, now - lastReportMs);
        long curGen  = logsGenerated.sum();
        long curSent = logsSent.sum();

        double genTps  = (curGen  - lastGenerated) * 1000.0 / elapsed;
        double sentTps = (curSent - lastSent)       * 1000.0 / elapsed;
        double mbSent  = bytesSent.sum() / 1_048_576.0;

        lastGenerated = curGen;
        lastSent      = curSent;
        lastReportMs  = now;

        // JVM Memory
        MemoryMXBean mem      = ManagementFactory.getMemoryMXBean();
        long         heapUsed = mem.getHeapMemoryUsage().getUsed() / 1_048_576;
        long         heapMax  = mem.getHeapMemoryUsage().getMax()  / 1_048_576;

        // CPU
        String cpuStr = "N/A";
        OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        if (os instanceof com.sun.management.OperatingSystemMXBean sunOs) {
            cpuStr = String.format("%.1f%%", sunOs.getProcessCpuLoad() * 100);
        }

        // GC
        List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
        long gcCount = gcBeans.stream()
            .mapToLong(GarbageCollectorMXBean::getCollectionCount)
            .filter(c -> c >= 0).sum();

        String r = String.format(
            "%n╔══════════════════════════════════════════════════════════╗" +
            "%n║                  CLIENT STATS REPORT                    ║" +
            "%n╠══════════════════════════════════════════════════════════╣" +
            "%n║  THROUGHPUT                                              ║" +
            "%n║    Generate TPS   : %8.0f logs/s                    ║" +
            "%n║    Send TPS       : %8.0f logs/s                    ║" +
            "%n╠══════════════════════════════════════════════════════════╣" +
            "%n║  TOTALS                                                  ║" +
            "%n║    Generated      : %,12d                         ║" +
            "%n║    Sent OK        : %,12d                         ║" +
            "%n║    Dropped(server): %,12d                         ║" +
            "%n╠══════════════════════════════════════════════════════════╣" +
            "%n║  HTTP                                                    ║" +
            "%n║    Requests       : %,12d                         ║" +
            "%n║    Errors         : %,12d                         ║" +
            "%n║    Retries        : %,12d                         ║" +
            "%n║    Data sent      : %8.2f MB                        ║" +
            "%n╠══════════════════════════════════════════════════════════╣" +
            "%n║  JVM / SYSTEM                                            ║" +
            "%n║    Heap           : %4d MB / %4d MB                  ║" +
            "%n║    CPU (process)  : %-10s                           ║" +
            "%n║    GC collections : %,12d                         ║" +
            "%n╚══════════════════════════════════════════════════════════╝",
            genTps, sentTps,
            curGen, curSent, logsDropped.sum(),
            httpRequests.sum(), httpErrors.sum(), httpRetries.sum(), mbSent,
            heapUsed, heapMax, cpuStr, gcCount);
        log.info(r);
        return r;
    }
}
