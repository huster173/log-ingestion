package com.logingestion.server.controller;

import com.logingestion.server.queue.LogQueue;
import com.logingestion.server.repository.LogRepository;
import com.logingestion.server.stats.ServerStats;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Thống kê realtime — trả về JSON để monitoring.
 *
 * GET /stats      → toàn bộ metrics
 *
 * Kết hợp với:
 *   GET /actuator/health   → Spring health check
 *   GET /actuator/metrics  → Micrometer metrics
 */
@RestController
@RequestMapping("/stats")
public class StatsController {

    private final ServerStats   stats;
    private final LogQueue      queue;
    private final LogRepository repository;

    public StatsController(ServerStats stats, LogQueue queue, LogRepository repository) {
        this.stats      = stats;
        this.queue      = queue;
        this.repository = repository;
    }

    @GetMapping
    public Map<String, Object> getStats() {
        Map<String, Object> result = new LinkedHashMap<>();

        // ── Throughput ─────────────────────────────────────────────────────
        Map<String, Object> throughput = new LinkedHashMap<>();
        throughput.put("receiveTps",      String.format("%.0f logs/s", stats.getReceiveTps()));
        throughput.put("insertTps",       String.format("%.0f logs/s", stats.getInsertTps()));
        result.put("throughput", throughput);

        // ── Pipeline ────────────────────────────────────────────────────────
        Map<String, Object> pipeline = new LinkedHashMap<>();
        pipeline.put("totalReceived",    stats.getTotalReceived());
        pipeline.put("totalInserted",    stats.getTotalInserted());
        pipeline.put("totalDropped",     stats.getTotalDropped());
        pipeline.put("batchesWritten",   stats.getBatchesWritten());
        pipeline.put("batchesFailed",    stats.getBatchesFailed());
        pipeline.put("retryAttempts",    stats.getRetryAttempts());
        pipeline.put("retriesExhausted", stats.getRetriesExhausted());
        long totalDB = repository.countAll();
        pipeline.put("totalInDB",        totalDB);
        result.put("pipeline", pipeline);

        // ── Queue ───────────────────────────────────────────────────────────
        Map<String, Object> queueInfo = new LinkedHashMap<>();
        queueInfo.put("currentDepth", queue.size());
        queueInfo.put("peakDepth",    stats.getQueuePeak());
        queueInfo.put("overloaded",   queue.isOverloaded());
        result.put("queue", queueInfo);

        // ── HTTP ────────────────────────────────────────────────────────────
        Map<String, Object> http = new LinkedHashMap<>();
        http.put("totalRequests", stats.getHttpRequests());
        http.put("totalErrors",   stats.getHttpErrors());
        result.put("http", http);

        // ── JVM Memory ──────────────────────────────────────────────────────
        MemoryMXBean mem = ManagementFactory.getMemoryMXBean();
        long heapUsed  = mem.getHeapMemoryUsage().getUsed()      / 1_048_576;
        long heapMax   = mem.getHeapMemoryUsage().getMax()       / 1_048_576;
        long nonHeap   = mem.getNonHeapMemoryUsage().getUsed()   / 1_048_576;

        Map<String, Object> jvm = new LinkedHashMap<>();
        jvm.put("heapUsedMb",  heapUsed);
        jvm.put("heapMaxMb",   heapMax);
        jvm.put("nonHeapMb",   nonHeap);
        jvm.put("heapPercent", heapMax > 0
            ? String.format("%.1f%%", heapUsed * 100.0 / heapMax) : "N/A");

        // GC stats
        List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
        long totalGcCount = gcBeans.stream().mapToLong(GarbageCollectorMXBean::getCollectionCount)
                                   .filter(c -> c >= 0).sum();
        long totalGcTimeMs = gcBeans.stream().mapToLong(GarbageCollectorMXBean::getCollectionTime)
                                    .filter(t -> t >= 0).sum();
        jvm.put("gcCount",   totalGcCount);
        jvm.put("gcTimeMs",  totalGcTimeMs);
        result.put("jvm", jvm);

        // ── System ──────────────────────────────────────────────────────────
        OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        Map<String, Object> system = new LinkedHashMap<>();
        system.put("availableProcessors", os.getAvailableProcessors());
        system.put("systemLoadAverage",
            os.getSystemLoadAverage() >= 0
                ? String.format("%.2f", os.getSystemLoadAverage()) : "N/A");

        // Process CPU (nếu là com.sun.management.OperatingSystemMXBean)
        if (os instanceof com.sun.management.OperatingSystemMXBean sunOs) {
            double processCpu = sunOs.getProcessCpuLoad() * 100;
            double systemCpu  = sunOs.getCpuLoad() * 100;
            system.put("processCpuPercent", String.format("%.1f%%", processCpu));
            system.put("systemCpuPercent",  String.format("%.1f%%", systemCpu));
        }
        result.put("system", system);

        return result;
    }
}
