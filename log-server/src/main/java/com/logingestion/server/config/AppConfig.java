package com.logingestion.server.config;

import com.logingestion.server.processor.BatchProcessor;
import com.logingestion.server.queue.LogQueue;
import com.logingestion.server.repository.LogRepository;
import com.logingestion.server.stats.ServerStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring bean wiring — kết nối các component của hệ thống.
 *
 * BatchProcessor dùng initMethod/destroyMethod để tự động
 * start workers khi Spring khởi động và drain queue khi shutdown.
 */
@Configuration
public class AppConfig {

    private static final Logger log = LoggerFactory.getLogger(AppConfig.class);

    @Bean
    public LogQueue logQueue(ServerProperties props, ServerStats stats) {
        log.info("Initializing LogQueue: capacity={}", props.getQueue().getCapacity());
        return new LogQueue(props, stats);
    }

    /**
     * BatchProcessor:
     * - initMethod="start"   → gọi khi Spring context ready
     * - destroyMethod="stop" → gọi khi JVM shutdown (graceful drain)
     */
    @Bean(initMethod = "start", destroyMethod = "stop")
    public BatchProcessor batchProcessor(ServerProperties props, LogQueue queue,
                                         LogRepository repository, ServerStats stats) {
        log.info("Initializing BatchProcessor: threads={}, batchSize={}, flushMs={}",
            props.getBatch().getProcessorThreads(),
            props.getBatch().getSize(),
            props.getBatch().getFlushMs());
        return new BatchProcessor(props, queue, repository, stats);
    }
}
