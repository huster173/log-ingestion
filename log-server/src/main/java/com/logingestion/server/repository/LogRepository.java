package com.logingestion.server.repository;

import com.logingestion.common.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

/**
 * JDBC batch insert vào bảng access_logs.
 *
 * Sử dụng Spring JdbcTemplate + HikariCP connection pool.
 * Schema được tạo tự động từ schema.sql (spring.sql.init.mode=always).
 *
 * Để switch sang PostgreSQL:
 *   1. Thêm dependency postgresql vào log-server/pom.xml
 *   2. Cập nhật spring.datasource.url trong application.yml
 */
@Repository
public class LogRepository {

    private static final Logger log = LoggerFactory.getLogger(LogRepository.class);

    private static final String INSERT_SQL =
        "INSERT INTO access_logs (id, ts, ip, method, path, status) VALUES (?,?,?,?,?,?)";

    private final JdbcTemplate jdbc;

    public LogRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * JDBC batch insert — hiệu quả hơn N lần so với insert đơn lẻ.
     *
     * Dùng BatchPreparedStatementSetter → executeBatch() → 1 round-trip DB.
     * Transaction tự động wrap toàn bộ batch.
     *
     * @param entries list LogEntry cần insert
     * @return số rows inserted thành công
     */
    public int insertBatch(List<LogEntry> entries) {
        if (entries == null || entries.isEmpty()) return 0;

        int[] results = jdbc.batchUpdate(INSERT_SQL, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                LogEntry e = entries.get(i);
                ps.setString(   1, e.getId());
                ps.setTimestamp(2, Timestamp.from(e.getTimestamp()));
                ps.setString(   3, e.getIp());
                ps.setString(   4, e.getMethod());
                ps.setString(   5, e.getPath());
                ps.setInt(      6, e.getStatus());
            }

            @Override
            public int getBatchSize() {
                return entries.size();
            }
        });

        // Đếm số rows thành công
        int ok = 0;
        for (int r : results) {
            if (r >= 0 || r == PreparedStatement.SUCCESS_NO_INFO) ok++;
        }
        return ok;
    }

    /** Đếm tổng số log đã lưu trong DB — dùng cho stats */
    public long countAll() {
        Long count = jdbc.queryForObject("SELECT COUNT(*) FROM access_logs", Long.class);
        return count != null ? count : 0L;
    }
}
