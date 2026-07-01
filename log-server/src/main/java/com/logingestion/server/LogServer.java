package com.logingestion.server;

import com.logingestion.server.config.ServerConfig;
import com.logingestion.server.database.LogDatabase;
import com.logingestion.server.http.LogHttpServer;
import com.logingestion.server.processor.BatchProcessor;
import com.logingestion.server.queue.LogQueue;
import com.logingestion.server.stats.ServerStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Log Ingestion Server — standalone HTTP service.
 *
 * Architecture:
 *
 *   [Client App A]──┐
 *   [Client App B]──┤  POST /logs/batch (JSON)
 *   [Client App C]──┘
 *          │
 *   [LogHttpServer]  ← JDK HttpServer, N handler threads
 *          │  offerBatch() — non-blocking
 *          ▼
 *   [LogQueue]  ← bounded, back-pressure, drop at 95%
 *          │  drain(batchSize, timeout)
 *          ▼
 *   [BatchProcessor]  ← M worker threads
 *          │  insertBatch() + retry exponential backoff
 *          ▼
 *   [LogDatabase]  ← HikariCP → H2 / PostgreSQL / MySQL
 *
 * Run:
 *   java -jar log-server.jar --port 8080 --processors 4 --batch-size 500
 */
public class LogServer {

    private static final Logger log = LoggerFactory.getLogger(LogServer.class);

    public static void main(String[] args) throws Exception {
        log.info("╔══════════════════════════════════════╗");
        log.info("║     Log Ingestion Server Starting    ║");
        log.info("╚══════════════════════════════════════╝");

        // 1. Config
        ServerConfig config = buildConfig(args);
        log.info("Config: {}", config);

        // 2. Wire
        ServerStats    stats     = new ServerStats();
        LogDatabase    db        = new LogDatabase(config);
        LogQueue       queue     = new LogQueue(config, stats);
        BatchProcessor processor = new BatchProcessor(config, queue, db, stats);
        LogHttpServer  http      = new LogHttpServer(config, queue, stats);

        // 3. Stats reporter
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "stats-reporter"); t.setDaemon(true); return t;
        });
        scheduler.scheduleAtFixedRate(stats::report,
            config.getStatsIntervalMs(), config.getStatsIntervalMs(), TimeUnit.MILLISECONDS);

        // 4. Graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("🛑 Shutdown signal — draining...");
            http.stop();
            processor.stop();
            scheduler.shutdownNow();
            log.info("Final DB count: {}", db.countAll());
            db.close();
        }, "shutdown-hook"));

        // 5. Start
        processor.start();
        http.start();

        log.info("✅ Server ready. Ctrl+C to stop.");
        Thread.currentThread().join(); // block forever
    }

    private static ServerConfig buildConfig(String[] args) {
        ServerConfig c = new ServerConfig();
        c.setHttpPort((int)        arg(args, "--port",          c.getHttpPort()));
        c.setHttpThreads((int)     arg(args, "--http-threads",  c.getHttpThreads()));
        c.setQueueCapacity((int)   arg(args, "--queue",         c.getQueueCapacity()));
        c.setBatchSize((int)       arg(args, "--batch-size",    c.getBatchSize()));
        c.setBatchFlushMs(          arg(args, "--flush-ms",     c.getBatchFlushMs()));
        c.setProcessorThreads((int)arg(args, "--processors",    c.getProcessorThreads()));
        c.setDbPoolMax((int)       arg(args, "--db-pool",       c.getDbPoolMax()));
        c.setStatsIntervalMs(      arg(args, "--stats-ms",      c.getStatsIntervalMs()));
        return c;
    }

    private static long arg(String[] args, String key, long def) {
        if (args == null) return def;
        for (int i = 0; i < args.length - 1; i++)
            if (key.equals(args[i])) { try { return Long.parseLong(args[i+1]); } catch (Exception e) { break; } }
        return def;
    }
}
