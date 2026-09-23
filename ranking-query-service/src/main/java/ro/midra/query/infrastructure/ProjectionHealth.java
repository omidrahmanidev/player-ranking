package ro.midra.query.infrastructure;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

@Component("projectionHealthIndicator")
public class ProjectionHealth implements HealthIndicator {
    private final RedisRankingReader reader;

    public ProjectionHealth(RedisRankingReader reader) {
        this.reader = reader;
    }

    @Override
    public Health health() {
        try {
            reader.read("top", "");
            return Health.up().build();
        } catch (RuntimeException error) {
            return Health.down().withDetail("projection", "unavailable").build();
        }
    }
}
