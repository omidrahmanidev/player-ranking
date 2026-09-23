package ro.midra.stream.infrastructure;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class AuditConsumerConfiguration {
    /**
     * Keep offsets unacknowledged during SQL outages instead of silently dropping audit records.
     */
    @Bean
    DefaultErrorHandler auditErrorHandler() {
        return new DefaultErrorHandler(new FixedBackOff(1000, FixedBackOff.UNLIMITED_ATTEMPTS));
    }
}
