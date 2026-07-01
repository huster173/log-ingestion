package com.logingestion.client;

import com.logingestion.client.config.ClientConfig;
import com.logingestion.client.producer.LogGenerator;
import com.logingestion.client.sender.LogSender;
import com.logingestion.client.stats.ClientStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Log Client — standalone app, chạy riêng với Log Server.
 *
 * Flow:
 *   [LogGenerator]  ← sinh log theo TPS target
 *       │ gom thành client-side batch
 *       ▼
 *   [LogSender]  ← HTTP POST /logs/batch → Server
 *       │ retry on 503/network error
 *       ▼
 *   [Log Server]  ← app riêng, port 8080
 *
 * Chạy:
 *   # Start server trước
 *   java -jar log-server.jar --port 8080
 *
 *   # Start client (app riêng, terminal khác)
 *   java -jar log-client.jar --server http://localhost:8080 --tps 1000
 *
 *   # Nhiều client cùng lúc (stress test)
 *   java -jar log-client.jar --server http://10.0.1.5:8080 --tps 5000 --threads 8
 */
public class LogClient {

    private static final Logger log = LoggerFactory.getLogger(LogClient.class);

    public static void main(String[] args) throws InterruptedException {


        // 1. Config
        ClientConfig config = buildConfig(args);
        log.info("Config: {}", config);

        // 2. Wire
        ClientStats  stats     = new ClientStats();
        LogSender    sender    = new LogSender(config, stats);
        LogGenerator generator = new LogGenerator(config, sender, stats);

        // 3. Stats reporter
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "client-stats"); t.setDaemon(true); return t;
        });
        scheduler.scheduleAtFixedRate(stats::report,
            config.getStatsIntervalMs(), config.getStatsIntervalMs(), TimeUnit.MILLISECONDS);

        // 4. Graceful shutdown-
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("🛑 Stopping client...");
            generator.stop();
            scheduler.shutdownNow();
            log.info("=== FINAL CLIENT STATS ===");
            stats.report();
        }, "client-shutdown"));

        // 5. Start
        generator.start();

        // 6. Run for duration or forever
        long duration = config.getDurationSeconds();
        if (duration > 0) {
            log.info("Running for {} seconds...", duration);
            Thread.sleep(duration * 1_000);
            log.info("Duration complete.");
            System.exit(0);
        } else {
            log.info("✅ Client running. Ctrl+C to stop.");
            Thread.currentThread().join();
        }
    }

    private static ClientConfig buildConfig(String[] args) {
        ClientConfig c = new ClientConfig();
        String server = argStr(args, "--server", null);
        if (server != null) c.setServerUrl(server);
        c.setTargetTps((int)       arg(args, "--tps",          c.getTargetTps()));
        c.setProducerThreads((int) arg(args, "--threads",       c.getProducerThreads()));
        c.setClientBatchSize((int) arg(args, "--batch-size",    c.getClientBatchSize()));
        c.setClientFlushMs(        arg(args, "--flush-ms",      c.getClientFlushMs()));
        c.setHttpConnections((int) arg(args, "--connections",   c.getHttpConnections()));
        c.setDurationSeconds(      arg(args, "--duration",      c.getDurationSeconds()));
        c.setStatsIntervalMs(      arg(args, "--stats-ms",      c.getStatsIntervalMs()));
        return c;
    }

    private static long arg(String[] args, String key, long def) {
        for (int i = 0; i < args.length - 1; i++)
            if (key.equals(args[i])) { try { return Long.parseLong(args[i+1]); } catch (Exception e) { break; } }
        return def;
    }

    private static String argStr(String[] args, String key, String def) {
        for (int i = 0; i < args.length - 1; i++)
            if (key.equals(args[i])) return args[i+1];
        return def;
    }
}
