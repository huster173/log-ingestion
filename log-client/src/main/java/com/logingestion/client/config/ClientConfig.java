package com.logingestion.client.config;

/**
 * All client-side tuneable parameters.
 * java -jar log-client.jar --server http://localhost:8080 --tps 1000
 */
public class ClientConfig {

    // Server target
    private String serverUrl        = "http://localhost:8080";

    // Rate
    private int    targetTps        = 1_000;
    private int    producerThreads  = 4;

    // Batching on client side (gom trước khi gửi HTTP)
    private int    clientBatchSize  = 100;   // gom 100 logs → 1 HTTP request
    private long   clientFlushMs    = 100;   // hoặc flush sau 100ms

    // HTTP connection pool
    private int    httpConnections  = 8;
    private int    connectTimeoutMs = 3_000;
    private int    readTimeoutMs    = 5_000;

    // Retry khi server trả về 503
    private int    maxSendRetries   = 3;
    private long   sendBackoffMs    = 200;

    // Stats
    private long   statsIntervalMs  = 5_000;

    // Run duration (0 = infinite)
    private long   durationSeconds  = 0;

    // ── Getters / Setters ───────────────────────────────────────────────────
    public String getServerUrl()            { return serverUrl; }
    public void   setServerUrl(String v)    { serverUrl = v; }

    public int    getTargetTps()            { return targetTps; }
    public void   setTargetTps(int v)       { targetTps = v; }

    public int    getProducerThreads()      { return producerThreads; }
    public void   setProducerThreads(int v) { producerThreads = v; }

    public int    getClientBatchSize()      { return clientBatchSize; }
    public void   setClientBatchSize(int v) { clientBatchSize = v; }

    public long   getClientFlushMs()        { return clientFlushMs; }
    public void   setClientFlushMs(long v)  { clientFlushMs = v; }

    public int    getHttpConnections()      { return httpConnections; }
    public void   setHttpConnections(int v) { httpConnections = v; }

    public int    getConnectTimeoutMs()     { return connectTimeoutMs; }
    public int    getReadTimeoutMs()        { return readTimeoutMs; }

    public int    getMaxSendRetries()       { return maxSendRetries; }
    public long   getSendBackoffMs()        { return sendBackoffMs; }

    public long   getStatsIntervalMs()      { return statsIntervalMs; }
    public void   setStatsIntervalMs(long v){ statsIntervalMs = v; }

    public long   getDurationSeconds()      { return durationSeconds; }
    public void   setDurationSeconds(long v){ durationSeconds = v; }

    @Override
    public String toString() {
        return String.format(
            "ClientConfig{server=%s, tps=%d, threads=%d, batchSize=%d, flushMs=%d}",
            serverUrl, targetTps, producerThreads, clientBatchSize, clientFlushMs);
    }
}
