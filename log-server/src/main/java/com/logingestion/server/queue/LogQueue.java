package com.logingestion.server.queue;

import com.logingestion.common.LogEntry;
import com.logingestion.server.config.ServerConfig;
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
 * Bounded internal queue.
 * HTTP handler threads push here; BatchProcessor workers drain from here.
 */
public class LogQueue {

    private static final Logger log = LoggerFactory.getLogger(LogQueue.class);

    private final BlockingQueue<LogEntry> queue;
    private final ServerConfig            config;
    private final ServerStats             stats;
    private final AtomicBoolean           overloaded = new AtomicBoolean(false);

    public LogQueue(ServerConfig config, ServerStats stats) {
        this.config = config;
        this.stats  = stats;
        this.queue  = new ArrayBlockingQueue<>(config.getQueueCapacity());
    }

    /**
     * Push a batch of entries from an HTTP request.
     * Returns number of entries actually accepted (rest dropped).
     */
    public int offerBatch(List<LogEntry> entries) {
        int accepted = 0;
        for (LogEntry e : entries) {
            if (tryOffer(e)) accepted++;
        }
        return accepted;
    }

    private boolean tryOffer(LogEntry entry) {
        int    cap   = config.getQueueCapacity();
        int    cur   = queue.size();
        double ratio = (double) cur / cap;

        stats.updateQueueDepth(cur);

        if (ratio >= config.getQueueDropThreshold()) {
            if (overloaded.compareAndSet(false, true)) {
                log.warn("⚠️  OVERLOAD: queue {:.0f}% full — dropping entries", ratio * 100);
            }
            stats.recordDropped(1);
            return false;
        }

        if (ratio >= config.getQueueWarnThreshold()) {
            overloaded.compareAndSet(false, true);
        } else {
            overloaded.set(false);
        }

        boolean ok = queue.offer(entry);
        if (!ok) stats.recordDropped(1);
        return ok;
    }

    /** Drain up to maxItems, blocking up to timeoutMs for the first entry. */
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
