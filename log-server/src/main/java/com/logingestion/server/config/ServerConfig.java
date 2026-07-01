package com.logingestion.server.config;

/**
 * All server-side tuneable parameters.
 * Pass as CLI args: --port 8080 --batch-size 500
 */
public class ServerConfig {

    // HTTP server
    private int    httpPort           = 8080;
    private int    httpThreads        = 8;       // threads handling incoming HTTP connections
    private int    maxRequestBodyKb   = 512;     // max JSON body per request

    // Internal queue
    private int    queueCapacity      = 200_000;
    private double queueWarnThreshold = 0.75;
    private double queueDropThreshold = 0.95;

    // Batch processor
    private int    batchSize          = 500;
    private long   batchFlushMs       = 200;
    private int    processorThreads   = 4;

    // Retry
    private int    maxRetries         = 3;
    private long   initialBackoffMs   = 100;
    private long   maxBackoffMs       = 2_000;

    // DB pool
    private int    dbPoolMin          = 4;
    private int    dbPoolMax          = 16;

    // Stats
    private long   statsIntervalMs    = 5_000;

    // ── Getters / Setters ───────────────────────────────────────────────────
    public int    getHttpPort()            { return httpPort; }
    public void   setHttpPort(int v)       { httpPort = v; }

    public int    getHttpThreads()         { return httpThreads; }
    public void   setHttpThreads(int v)    { httpThreads = v; }

    public int    getMaxRequestBodyKb()    { return maxRequestBodyKb; }
    public void   setMaxRequestBodyKb(int v){ maxRequestBodyKb = v; }

    public int    getQueueCapacity()       { return queueCapacity; }
    public void   setQueueCapacity(int v)  { queueCapacity = v; }

    public double getQueueWarnThreshold()  { return queueWarnThreshold; }
    public double getQueueDropThreshold()  { return queueDropThreshold; }

    public int    getBatchSize()           { return batchSize; }
    public void   setBatchSize(int v)      { batchSize = v; }

    public long   getBatchFlushMs()        { return batchFlushMs; }
    public void   setBatchFlushMs(long v)  { batchFlushMs = v; }

    public int    getProcessorThreads()    { return processorThreads; }
    public void   setProcessorThreads(int v){ processorThreads = v; }

    public int    getMaxRetries()          { return maxRetries; }
    public long   getInitialBackoffMs()    { return initialBackoffMs; }
    public long   getMaxBackoffMs()        { return maxBackoffMs; }

    public int    getDbPoolMin()           { return dbPoolMin; }
    public int    getDbPoolMax()           { return dbPoolMax; }
    public void   setDbPoolMax(int v)      { dbPoolMax = v; }

    public long   getStatsIntervalMs()     { return statsIntervalMs; }
    public void   setStatsIntervalMs(long v){ statsIntervalMs = v; }

    @Override
    public String toString() {
        return String.format(
            "ServerConfig{port=%d, httpThreads=%d, queue=%d, batch=%d, flush=%dms, processors=%d}",
            httpPort, httpThreads, queueCapacity, batchSize, batchFlushMs, processorThreads);
    }
}
