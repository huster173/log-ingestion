package com.logingestion.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.logingestion.server.config.ServerProperties;

/**
 * Log Ingestion Server — Spring Boot 3.3.1, Java 21 (Virtual Threads)
 *
 * ┌─────────────────────────────────────────────────────────────────┐
 * │                    ARCHITECTURE                                 │
 * │                                                                 │
 * │  [Client]──POST /logs/batch──► [LogController]                 │
 * │                                       │ offerBatch()           │
 * │                                       ▼                        │
 * │                             [LogQueue]                         │
 * │                             ArrayBlockingQueue(200K)           │
 * │                             back-pressure: warn@75% drop@95%  │
 * │                                       │ drain(500, 200ms)      │
 * │                                       ▼                        │
 * │                           [BatchProcessor]                     │
 * │                           4 worker threads                     │
 * │                           exponential backoff retry (3x)      │
 * │                                       │ insertBatch()          │
 * │                                       ▼                        │
 * │                           [LogRepository]                      │
 * │                           JdbcTemplate batch insert           │
 * │                           HikariCP pool                        │
 * │                                       │                        │
 * │                                       ▼                        │
 * │                             H2 / PostgreSQL                    │
 * │                                                                 │
 * │  GET /stats           → JSON metrics                           │
 * │  GET /actuator/health → Spring health check                    │
 * │  GET /h2-console      → H2 DB web UI (dev)                    │
 * └─────────────────────────────────────────────────────────────────┘
 *
 * Chạy:
 *   java -jar log-server-2.0.0.jar
 *   java -jar log-server-2.0.0.jar --server.port=9090
 *   java -jar log-server-2.0.0.jar --log.server.batch.size=1000
 */
@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(ServerProperties.class)
public class LogServerApplication {

    private static final Logger log = LoggerFactory.getLogger(LogServerApplication.class);

    public static void main(String[] args) {
        log.info("╔══════════════════════════════════════════╗");
        log.info("║   Log Ingestion Server v2.0 Starting     ║");
        log.info("║   Java 21 Virtual Threads                ║");
        log.info("║   Spring Boot 3.3.1 + HikariCP           ║");
        log.info("╚══════════════════════════════════════════╝");
        SpringApplication.run(LogServerApplication.class, args);
    }
}
