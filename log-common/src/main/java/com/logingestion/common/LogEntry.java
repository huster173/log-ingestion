package com.logingestion.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.UUID;

/**
 * Shared DTO serialised as JSON over HTTP between client and server.
 *
 * Wire format (JSON):
 * {
 *   "id":        "uuid",
 *   "timestamp": "2024-01-15T10:30:00.123Z",
 *   "ip":        "10.0.1.42",
 *   "method":    "GET",
 *   "path":      "/api/users",
 *   "status":    200
 * }
 */
public final class LogEntry {

    private final String  id;
    private final Instant timestamp;
    private final String  ip;
    private final String  method;
    private final String  path;
    private final int     status;

    /** Used by client to create a new entry */
    public LogEntry(Instant timestamp, String ip, String method, String path, int status) {
        this.id        = UUID.randomUUID().toString();
        this.timestamp = timestamp;
        this.ip        = ip;
        this.method    = method;
        this.path      = path;
        this.status    = status;
    }

    /** Used by Jackson to deserialise on server side */
    @JsonCreator
    public LogEntry(
        @JsonProperty("id")        String  id,
        @JsonProperty("timestamp") Instant timestamp,
        @JsonProperty("ip")        String  ip,
        @JsonProperty("method")    String  method,
        @JsonProperty("path")      String  path,
        @JsonProperty("status")    int     status
    ) {
        this.id        = id;
        this.timestamp = timestamp;
        this.ip        = ip;
        this.method    = method;
        this.path      = path;
        this.status    = status;
    }

    public String  getId()        { return id; }
    public Instant getTimestamp() { return timestamp; }
    public String  getIp()        { return ip; }
    public String  getMethod()    { return method; }
    public String  getPath()      { return path; }
    public int     getStatus()    { return status; }

    @Override
    public String toString() {
        return String.format("[%s] %s %s %s %d", timestamp, ip, method, path, status);
    }
}
