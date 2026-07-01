package com.logingestion.server.stats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class ServerStats {

    private static final Logger log = LoggerFactory.getLogger(ServerStats.class);

    // Incoming HTTP
    private final LongAdder httpRequests   = new LongAdder();
    private final LongAdder logsReceived   = new LongAdder();
    private final LongAdder logsDropped    = new LongAdder();
    private final LongAdder httpErrors     = new LongAdder();

    // Queue
    private final AtomicLong queueDepth    = new AtomicLong();
    private final AtomicLong queuePeak     = new AtomicLong();

    // Processing
    private final LongAdder logsInserted   = new LongAdder();
    private final LongAdder batchesWritten = new LongAdder();
    private final LongAdder batchesFailed  = new LongAdder();
    private final LongAdder retryAttempts  = new LongAdder();
    private final LongAdder retriesExhausted = new LongAdder();

    // For TPS calculation
    private volatile long lastReceived = 0;
    private volatile long lastInserted = 0;
    private volatile long lastReportMs = System.currentTimeMillis();

    public void recordHttpRequest()         { httpRequests.increment(); }
    public void recordReceived(int n)       { logsReceived.add(n); }
    public void recordDropped(int n)        { logsDropped.add(n); }
    public void recordHttpError()           { httpErrors.increment(); }
    public void recordInserted(int n)       { logsInserted.add(n); batchesWritten.increment(); }
    public void recordBatchFailed()         { batchesFailed.increment(); }
    public void recordRetryAttempt()        { retryAttempts.increment(); }
    public void recordRetriesExhausted()    { retriesExhausted.increment(); }

    public void updateQueueDepth(long d) {
        queueDepth.set(d);
        queuePeak.updateAndGet(p -> Math.max(p, d));
    }

    public long getQueueDepth()     { return queueDepth.get(); }
    public long getTotalReceived()  { return logsReceived.sum(); }
    public long getTotalInserted()  { return logsInserted.sum(); }
    public long getTotalDropped()   { return logsDropped.sum(); }

    public String report() {
        long now      = System.currentTimeMillis();
        long elapsed  = Math.max(1, now - lastReportMs);
        long curRecv  = logsReceived.sum();
        long curIns   = logsInserted.sum();

        double recvTps = (curRecv - lastReceived) * 1000.0 / elapsed;
        double insTps  = (curIns  - lastInserted) * 1000.0 / elapsed;

        lastReceived = curRecv;
        lastInserted = curIns;
        lastReportMs = now;

        long avgBatch = batchesWritten.sum() > 0
            ? logsInserted.sum() / batchesWritten.sum() : 0;

        String r = String.format(
            "\n╔══════════════════════════════════════════════════════════╗\n" +
            "║                   SERVER STATS REPORT                    ║\n" +
            "╠══════════════════════════════════════════════════════════╣\n" +
            "║  THROUGHPUT                                               ║\n" +
            "║    Receive TPS    : %8.0f logs/s                     ║\n" +
            "║    Insert TPS     : %8.0f logs/s                     ║\n" +
            "╠══════════════════════════════════════════════════════════╣\n" +
            "║  HTTP                                                     ║\n" +
            "║    Requests       : %,12d                          ║\n" +
            "║    Errors         : %,12d                          ║\n" +
            "╠══════════════════════════════════════════════════════════╣\n" +
            "║  PIPELINE                                                 ║\n" +
            "║    Received       : %,12d                          ║\n" +
            "║    Inserted DB    : %,12d                          ║\n" +
            "║    Dropped        : %,12d                          ║\n" +
            "╠══════════════════════════════════════════════════════════╣\n" +
            "║  QUEUE                                                    ║\n" +
            "║    Depth now      : %,12d                          ║\n" +
            "║    Peak depth     : %,12d                          ║\n" +
            "╠══════════════════════════════════════════════════════════╣\n" +
            "║  BATCHES & RETRIES                                        ║\n" +
            "║    Batches written: %,12d  avg size: %,6d      ║\n" +
            "║    Batches failed : %,12d                          ║\n" +
            "║    Retry attempts : %,12d                          ║\n" +
            "║    Retries exh.   : %,12d                          ║\n" +
            "╚══════════════════════════════════════════════════════════╝",
            recvTps, insTps,
            httpRequests.sum(), httpErrors.sum(),
            curRecv, curIns, logsDropped.sum(),
            queueDepth.get(), queuePeak.get(),
            batchesWritten.sum(), avgBatch,
            batchesFailed.sum(), retryAttempts.sum(), retriesExhausted.sum()
        );
        log.info(r);
        return r;
    }
}
