package com.logingestion.server.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.logingestion.common.LogBatch;
import com.logingestion.common.LogEntry;
import com.logingestion.server.config.ServerConfig;
import com.logingestion.server.queue.LogQueue;
import com.logingestion.server.stats.ServerStats;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Executors;

/**
 * Lightweight HTTP server using JDK's built-in com.sun.net.httpserver.
 * Zero external dependencies — no Netty, no Jetty, no Spring.
 *
 * Endpoints:
 *   POST /logs          — receive a single LogEntry JSON
 *   POST /logs/batch    — receive a LogBatch JSON (preferred: fewer round-trips)
 *   GET  /health        — liveness check
 *   GET  /stats         — current metrics as plain text
 */
public class LogHttpServer {

    private static final Logger log = LoggerFactory.getLogger(LogHttpServer.class);

    private static final String PATH_LOG       = "/logs";
    private static final String PATH_BATCH     = "/logs/batch";
    private static final String PATH_HEALTH    = "/health";
    private static final String PATH_STATS     = "/stats";

    private final ServerConfig config;
    private final LogQueue     queue;
    private final ServerStats  stats;
    private final ObjectMapper mapper;
    private       HttpServer   server;

    public LogHttpServer(ServerConfig config, LogQueue queue, ServerStats stats) {
        this.config = config;
        this.queue  = queue;
        this.stats  = stats;
        this.mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());
    }

    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(config.getHttpPort()), 256);

        server.createContext(PATH_LOG,    this::handleSingleLog);
        server.createContext(PATH_BATCH,  this::handleBatchLog);
        server.createContext(PATH_HEALTH, this::handleHealth);
        server.createContext(PATH_STATS,  this::handleStats);

        // Thread pool for HTTP handlers — separate from batch processor threads
        server.setExecutor(Executors.newFixedThreadPool(config.getHttpThreads(),
            r -> {
                Thread t = new Thread(r, "http-handler-" + System.nanoTime());
                t.setDaemon(true);
                return t;
            }));

        server.start();
        log.info("✅ HTTP server listening on port {} ({} handler threads)",
            config.getHttpPort(), config.getHttpThreads());
        log.info("   POST {}       — single log entry", PATH_LOG);
        log.info("   POST {}  — batch of log entries (recommended)", PATH_BATCH);
        log.info("   GET  {}     — health check", PATH_HEALTH);
        log.info("   GET  {}       — live stats", PATH_STATS);
    }

    // ── Handlers ─────────────────────────────────────────────────────────────

    /** POST /logs — single entry, for simplicity / low-volume senders */
    private void handleSingleLog(HttpExchange ex) throws IOException {
        if (!"POST".equals(ex.getRequestMethod())) {
            respond(ex, 405, "Method Not Allowed");
            return;
        }
        stats.recordHttpRequest();
        try {
            byte[]   body  = readBody(ex, config.getMaxRequestBodyKb());
            LogEntry entry = mapper.readValue(body, LogEntry.class);
            int accepted   = queue.offerBatch(List.of(entry));
            stats.recordReceived(1);

            if (accepted == 1) {
                respond(ex, 202, "{\"status\":\"accepted\"}");
            } else {
                respond(ex, 503, "{\"status\":\"dropped\",\"reason\":\"queue_full\"}");
            }
        } catch (Exception e) {
            log.warn("Bad /logs request: {}", e.getMessage());
            stats.recordHttpError();
            respond(ex, 400, "{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    /**
     * POST /logs/batch — preferred endpoint.
     * Client sends {entries:[...]} — one HTTP call per client batch.
     * Returns 207 Multi-Status with per-batch acceptance count.
     */
    private void handleBatchLog(HttpExchange ex) throws IOException {
        if (!"POST".equals(ex.getRequestMethod())) {
            respond(ex, 405, "Method Not Allowed");
            return;
        }
        stats.recordHttpRequest();
        try {
            byte[]   body    = readBody(ex, config.getMaxRequestBodyKb());
            LogBatch batch   = mapper.readValue(body, LogBatch.class);
            int      total   = batch.size();
            int      accepted = queue.offerBatch(batch.getEntries());
            int      dropped  = total - accepted;

            stats.recordReceived(total);

            String responseJson = String.format(
                "{\"received\":%d,\"accepted\":%d,\"dropped\":%d}",
                total, accepted, dropped);

            // 202 if all accepted, 206 if partial, 503 if all dropped
            int statusCode = dropped == 0 ? 202 : (accepted > 0 ? 206 : 503);
            respond(ex, statusCode, responseJson);

        } catch (Exception e) {
            log.warn("Bad /logs/batch request: {}", e.getMessage());
            stats.recordHttpError();
            respond(ex, 400, "{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    /** GET /health — load balancer liveness probe */
    private void handleHealth(HttpExchange ex) throws IOException {
        boolean overloaded = queue.isOverloaded();
        int     code       = overloaded ? 503 : 200;
        String  body       = overloaded
            ? "{\"status\":\"degraded\",\"reason\":\"queue_pressure\"}"
            : "{\"status\":\"ok\",\"queue\":" + queue.size() + "}";
        respond(ex, code, body);
    }

    /** GET /stats — live metrics dump */
    private void handleStats(HttpExchange ex) throws IOException {
        respond(ex, 200, stats.report());
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private byte[] readBody(HttpExchange ex, int maxKb) throws IOException {
        int    maxBytes = maxKb * 1024;
        try (InputStream is = ex.getRequestBody()) {
            byte[] buf = is.readNBytes(maxBytes + 1);
            if (buf.length > maxBytes) {
                throw new IllegalArgumentException("Request body exceeds " + maxKb + "KB limit");
            }
            return buf;
        }
    }

    private void respond(HttpExchange ex, int statusCode, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        ex.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }

    public void stop() {
        if (server != null) {
            server.stop(2); // 2s grace period
            log.info("HTTP server stopped.");
        }
    }
}
