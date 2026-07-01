package com.logingestion.server.queue;

import com.logingestion.common.LogEntry;
import com.logingestion.server.config.ServerProperties;
import com.logingestion.server.stats.ServerStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Bounded internal queue với back-pressure mechanism.
 *
 * HTTP handler threads push entries vào đây (offerBatch).
 * BatchProcessor workers drain từ đây (drain).
 *
 * Back-pressure:
 *   - warn  @75%: log cảnh báo nhưng vẫn nhận
 *   - drop  @95%: từ chối nhận — trả về dropped count cho HTTP layer
 *
 * Thread-safe: ArrayBlockingQueue đảm bảo thread-safety.
 */
public class LogQueue {

    private static final Logger log = LoggerFactory.getLogger(LogQueue.class);

    private final BlockingQueue<LogEntry> queue;
    private final ServerProperties        props;
    private final ServerStats             stats;
    private final AtomicBoolean           overloaded = new AtomicBoolean(false);

    public LogQueue(ServerProperties props, ServerStats stats) {
        this.props = props;
        this.stats = stats;
        this.queue = new ArrayBlockingQueue<>(props.getQueue().getCapacity());
        log.info("LogQueue ready: capacity={}, warn={}%, drop={}%",
            props.getQueue().getCapacity(),
            (int)(props.getQueue().getWarnThreshold() * 100),
            (int)(props.getQueue().getDropThreshold() * 100));
    }

    /**
     * Thêm batch entries vào queue từ HTTP request — non-blocking.
     *
     * @param entries list log entries cần enqueue
     * @return số entries được chấp nhận (phần còn lại bị drop do backpressure)
     */
    public int offerBatch(List<LogEntry> entries) {
        int accepted = 0;
        for (LogEntry entry : entries) {
            if (tryOffer(entry)) accepted++;
        }
        return accepted;
    }

    private boolean tryOffer(LogEntry entry) {
        int    cap   = props.getQueue().getCapacity();
        int    cur   = queue.size();
        double ratio = (double) cur / cap;

        stats.updateQueueDepth(cur);

        // Drop threshold — từ chối entry mới
        if (ratio >= props.getQueue().getDropThreshold()) {
            if (overloaded.compareAndSet(false, true)) {
                log.warn("OVERLOAD: queue {}% full — dropping incoming entries",
                    (int)(ratio * 100));
            }
            stats.recordDropped(1);
            return false;
        }

        // Warn threshold — cảnh báo nhưng vẫn nhận
        if (ratio >= props.getQueue().getWarnThreshold()) {
            overloaded.set(true);
        } else {
            overloaded.set(false);
        }

        boolean ok = queue.offer(entry);
        if (!ok) {
            stats.recordDropped(1);
        }
        return ok;
    }

    /**
     * Drain batch cho BatchProcessor workers.
     * Block tối đa timeoutMs để chờ entry đầu tiên, sau đó drain ngay.
     *
     * @param maxItems  số entries tối đa trong 1 batch
     * @param timeoutMs chờ tối đa bao lâu cho entry đầu tiên
     * @return list entries (rỗng nếu timeout)
     */
    public List<LogEntry> drain(int maxItems, long timeoutMs) throws InterruptedException {
        List<LogEntry> batch = new ArrayList<>(maxItems);
        LogEntry first = queue.poll(timeoutMs, TimeUnit.MILLISECONDS);
        if (first == null) return batch;
        batch.add(first);
        queue.drainTo(batch, maxItems - 1);
        stats.updateQueueDepth(queue.size());
        return batch;
    }

    public int     size()         { return queue.size(); }
    public boolean isEmpty()      { return queue.isEmpty(); }
    public boolean isOverloaded() { return overloaded.get(); }
}
