package com.logingestion.client.stats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.LongAdder;

public class ClientStats {

    private static final Logger log = LoggerFactory.getLogger(ClientStats.class);

    private final LongAdder logsGenerated   = new LongAdder();
    private final LongAdder logsSent        = new LongAdder();
    private final LongAdder logsDropped     = new LongAdder(); // server returned 503/drop
    private final LongAdder httpRequests    = new LongAdder();
    private final LongAdder httpErrors      = new LongAdder();
    private final LongAdder httpRetries     = new LongAdder();
    private final LongAdder byteSent        = new LongAdder();

    private volatile long lastGenerated = 0;
    private volatile long lastSent      = 0;
    private volatile long lastReportMs  = System.currentTimeMillis();

    public void recordGenerated(int n)   { logsGenerated.add(n); }
    public void recordSent(int n)        { logsSent.add(n); }
    public void recordDropped(int n)     { logsDropped.add(n); }
    public void recordHttpRequest()      { httpRequests.increment(); }
    public void recordHttpError()        { httpErrors.increment(); }
    public void recordHttpRetry()        { httpRetries.increment(); }
    public void recordBytesSent(long b)  { byteSent.add(b); }

    public long getTotalGenerated()  { return logsGenerated.sum(); }
    public long getTotalSent()       { return logsSent.sum(); }

    public String report() {
        long now      = System.currentTimeMillis();
        long elapsed  = Math.max(1, now - lastReportMs);
        long curGen   = logsGenerated.sum();
        long curSent  = logsSent.sum();

        double genTps  = (curGen  - lastGenerated) * 1000.0 / elapsed;
        double sentTps = (curSent - lastSent)       * 1000.0 / elapsed;
        double mbSent  = byteSent.sum() / 1_048_576.0;

        lastGenerated = curGen;
        lastSent      = curSent;
        lastReportMs  = now;

        String r = String.format(
            "\n╔══════════════════════════════════════════════════════════╗\n" +
            "║                   CLIENT STATS REPORT                    ║\n" +
            "╠══════════════════════════════════════════════════════════╣\n" +
            "║  THROUGHPUT                                               ║\n" +
            "║    Generate TPS   : %8.0f logs/s                     ║\n" +
            "║    Send TPS       : %8.0f logs/s                     ║\n" +
            "╠══════════════════════════════════════════════════════════╣\n" +
            "║  TOTALS                                                   ║\n" +
            "║    Generated      : %,12d                          ║\n" +
            "║    Sent OK        : %,12d                          ║\n" +
            "║    Dropped(server): %,12d                          ║\n" +
            "╠══════════════════════════════════════════════════════════╣\n" +
            "║  HTTP                                                     ║\n" +
            "║    Requests       : %,12d                          ║\n" +
            "║    Errors         : %,12d                          ║\n" +
            "║    Retries        : %,12d                          ║\n" +
            "║    Data sent      : %8.2f MB                         ║\n" +
            "╚══════════════════════════════════════════════════════════╝",
            genTps, sentTps,
            curGen, curSent, logsDropped.sum(),
            httpRequests.sum(), httpErrors.sum(), httpRetries.sum(), mbSent
        );
        log.info(r);
        return r;
    }
}
