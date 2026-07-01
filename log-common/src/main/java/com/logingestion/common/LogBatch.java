package com.logingestion.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Client gom nhiều LogEntry thành 1 HTTP request để giảm overhead.
 *
 * Wire format:
 * {
 *   "entries": [ { ...LogEntry }, { ...LogEntry } ]
 * }
 */
public final class LogBatch {

    private final List<LogEntry> entries;

    public LogBatch(List<LogEntry> entries) {
        this.entries = entries;
    }

    @JsonCreator
    public static LogBatch of(@JsonProperty("entries") List<LogEntry> entries) {
        return new LogBatch(entries);
    }

    public List<LogEntry> getEntries() { return entries; }
    public int size()                  { return entries == null ? 0 : entries.size(); }
}
