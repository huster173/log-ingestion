package com.logingestion.server.controller;

import com.logingestion.common.LogBatch;
import com.logingestion.common.LogEntry;
import com.logingestion.server.queue.LogQueue;
import com.logingestion.server.stats.ServerStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * REST endpoint nhận log entries từ client.
 *
 * POST /logs       — Single log entry
 * POST /logs/batch — Batch entries (ưu tiên dùng endpoint này cho hiệu năng cao)
 *
 * Response codes:
 *   202 Accepted        — tất cả entries được chấp nhận
 *   206 Partial Content — một phần bị drop do backpressure
 *   503 Service Unavailable — queue đầy, không nhận được
 */
@RestController
@RequestMapping("/logs")
public class LogController {

    private static final Logger log = LoggerFactory.getLogger(LogController.class);

    private final LogQueue    queue;
    private final ServerStats stats;

    public LogController(LogQueue queue, ServerStats stats) {
        this.queue = queue;
        this.stats = stats;
    }

    /**
     * Nhận 1 log entry đơn lẻ.
     * Dùng cho testing, ưu tiên dùng /batch cho production.
     */
    @PostMapping
    public ResponseEntity<Map<String, Object>> ingest(@RequestBody LogEntry entry) {
        stats.recordHttpRequest();
        int accepted = queue.offerBatch(List.of(entry));
        stats.recordReceived(1);

        if (accepted == 0) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("received", 1, "accepted", 0, "dropped", 1,
                                 "reason", "queue_full"));
        }
        return ResponseEntity.accepted()
                .body(Map.of("received", 1, "accepted", 1, "dropped", 0));
    }

    /**
     * Nhận batch log entries — endpoint chính cho high throughput.
     *
     * Client nên gom N log entries thành 1 request thay vì N request riêng lẻ.
     * Điều này giảm đáng kể HTTP overhead và tăng throughput tổng thể.
     */
    @PostMapping("/batch")
    public ResponseEntity<Map<String, Object>> ingestBatch(@RequestBody LogBatch batch) {
        if (batch == null || batch.getEntries() == null || batch.getEntries().isEmpty()) {
            return ResponseEntity.badRequest()
                    .body(Map.of("error", "Empty batch"));
        }

        stats.recordHttpRequest();
        int total    = batch.getEntries().size();
        int accepted = queue.offerBatch(batch.getEntries());
        int dropped  = total - accepted;

        stats.recordReceived(accepted);
        if (dropped > 0) {
            stats.recordDropped(dropped);
        }

        if (accepted == 0) {
            // Queue hoàn toàn đầy — 503
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("received", total, "accepted", 0, "dropped", total,
                                 "reason", "queue_full"));
        }

        if (dropped > 0) {
            // Partial drop do backpressure — 206
            return ResponseEntity.status(HttpStatus.PARTIAL_CONTENT)
                    .body(Map.of("received", total, "accepted", accepted, "dropped", dropped));
        }

        // Tất cả được nhận — 202
        return ResponseEntity.accepted()
                .body(Map.of("received", total, "accepted", accepted, "dropped", 0));
    }
}
