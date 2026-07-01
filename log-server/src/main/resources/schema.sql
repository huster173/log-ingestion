-- Log Ingestion V2 — Database Schema
-- Tự động chạy khi Spring Boot startup (spring.sql.init.mode=always)
-- Tương thích H2 và PostgreSQL

CREATE TABLE IF NOT EXISTS access_logs (
    id          VARCHAR(36)  NOT NULL PRIMARY KEY,
    ts          TIMESTAMP    NOT NULL,
    ip          VARCHAR(45)  NOT NULL,
    method      VARCHAR(10)  NOT NULL,
    path        VARCHAR(512) NOT NULL,
    status      SMALLINT     NOT NULL,
    inserted_at TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);

-- Index để tăng tốc query theo thời gian, status, IP
CREATE INDEX IF NOT EXISTS idx_access_logs_ts     ON access_logs(ts);
CREATE INDEX IF NOT EXISTS idx_access_logs_status ON access_logs(status);
CREATE INDEX IF NOT EXISTS idx_access_logs_ip     ON access_logs(ip);
