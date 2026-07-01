package com.logingestion.server.database;

import com.logingestion.common.LogEntry;
import com.logingestion.server.config.ServerConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;

/**
 * JDBC batch insert with HikariCP.
 * Swap JDBC URL for PostgreSQL/MySQL — zero other changes.
 */
public class LogDatabase {

    private static final Logger log = LoggerFactory.getLogger(LogDatabase.class);

    private static final String INSERT_SQL =
        "INSERT INTO access_logs (id, ts, ip, method, path, status) VALUES (?,?,?,?,?,?)";

    private final HikariDataSource ds;

    public LogDatabase(ServerConfig config) {
        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl("jdbc:h2:mem:logs;DB_CLOSE_DELAY=-1;MODE=PostgreSQL");
        hc.setUsername("sa");
        hc.setPassword("");
        hc.setDriverClassName("org.h2.Driver");
        hc.setMinimumIdle(config.getDbPoolMin());
        hc.setMaximumPoolSize(config.getDbPoolMax());
        hc.setConnectionTimeout(3_000);
        hc.setPoolName("LogDB");
        hc.addDataSourceProperty("cachePrepStmts", "true");
        this.ds = new HikariDataSource(hc);
        initSchema();
        log.info("DB ready. Pool: {}-{}", config.getDbPoolMin(), config.getDbPoolMax());
    }

    private void initSchema() {
        String[] ddl = {
            "CREATE TABLE IF NOT EXISTS access_logs (" +
            "  id VARCHAR(36) NOT NULL PRIMARY KEY," +
            "  ts TIMESTAMP NOT NULL," +
            "  ip VARCHAR(45) NOT NULL," +
            "  method VARCHAR(10) NOT NULL," +
            "  path VARCHAR(512) NOT NULL," +
            "  status SMALLINT NOT NULL," +
            "  inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
            "CREATE INDEX IF NOT EXISTS idx_ts     ON access_logs(ts)",
            "CREATE INDEX IF NOT EXISTS idx_status ON access_logs(status)",
            "CREATE INDEX IF NOT EXISTS idx_ip     ON access_logs(ip)"
        };
        try (Connection c = ds.getConnection(); Statement s = c.createStatement()) {
            for (String sql : ddl) s.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException("Schema init failed", e);
        }
    }

    public int insertBatch(List<LogEntry> entries) throws SQLException {
        if (entries == null || entries.isEmpty()) return 0;
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(INSERT_SQL)) {
            c.setAutoCommit(false);
            for (LogEntry e : entries) {
                ps.setString(1, e.getId());
                ps.setTimestamp(2, Timestamp.from(e.getTimestamp()));
                ps.setString(3, e.getIp());
                ps.setString(4, e.getMethod());
                ps.setString(5, e.getPath());
                ps.setInt(6, e.getStatus());
                ps.addBatch();
            }
            int[] res = ps.executeBatch();
            c.commit();
            int ok = 0;
            for (int r : res) if (r >= 0 || r == Statement.SUCCESS_NO_INFO) ok++;
            return ok;
        }
    }

    public long countAll() {
        try (Connection c = ds.getConnection();
             ResultSet rs = c.createStatement().executeQuery("SELECT COUNT(*) FROM access_logs")) {
            return rs.next() ? rs.getLong(1) : 0;
        } catch (SQLException e) { return -1; }
    }

    public void close() {
        if (ds != null && !ds.isClosed()) ds.close();
    }
}
