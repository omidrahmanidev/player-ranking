package ro.midra.stream.infrastructure;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.context.SmartLifecycle;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import ro.midra.stream.domain.WindowPolicy;

import java.time.Clock;
import java.time.Duration;
import java.util.Properties;

/**
 * Separate application IDs isolate projection recovery from score calculation.
 */
@Component("streamsHealthIndicator")
public class StreamRuntime implements SmartLifecycle, HealthIndicator {
    private static final Logger LOG = LoggerFactory.getLogger(StreamRuntime.class);
    private final KafkaStreams calculator, projector;
    private volatile boolean running;

    public StreamRuntime(
            Clock clock,
            StringRedisTemplate redis,
            MeterRegistry metrics,
            @Value("${spring.kafka.bootstrap-servers}") String bootstrap,
            @Value("${ranking.state-dir:.state}") String stateDir,
            @Value("${ranking.threads:2}") int threads,
            @Value("${ranking.replication-factor:3}") int replication,
            @Value("${ranking.topics.scores:score-events-v1}") String scores,
            @Value("${ranking.topics.totals:ranking-totals-v1}") String totals,
            @Value("${ranking.topics.audit:ranking-audit-v1}") String audit) {
        calculator =
                new KafkaStreams(
                        RankingTopology.build(clock, WindowPolicy.standard(), scores, totals, audit),
                        properties(bootstrap, stateDir, threads, replication, "player-ranking-calculator-v1"));
        projector =
                new KafkaStreams(
                        ProjectionTopology.build(totals, redis, metrics),
                        properties(bootstrap, stateDir, threads, replication, "player-ranking-projection-v1"));
        observe(calculator, "calculator", metrics);
        observe(projector, "projector", metrics);
    }

    public static Properties properties(
            String bootstrap, String stateDir, int threads, int replication, String application) {
        var properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, application);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, threads);
        properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, replication);
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 250);
        properties.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, BoundedRocksDb.class);
        properties.put(StreamsConfig.consumerPrefix("auto.offset.reset"), "earliest");
        properties.put(StreamsConfig.consumerPrefix("max.poll.interval.ms"), 300_000);
        return properties;
    }

    private void observe(KafkaStreams streams, String name, MeterRegistry metrics) {
        streams.setStateListener(
                (next, previous) -> LOG.info("Streams {} state {} -> {}", name, previous, next));
        streams.setUncaughtExceptionHandler(
                error -> {
                    LOG.error("Streams {} failed", name, error);
                    return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
                });
        metrics.gauge(
                "ranking.streams.running",
                java.util.List.of(io.micrometer.core.instrument.Tag.of("topology", name)),
                streams,
                value -> value.state() == KafkaStreams.State.RUNNING ? 1 : 0);
    }

    @Override
    public void start() {
        calculator.start();
        projector.start();
        running = true;
    }

    @Override
    public void stop() {
        calculator.close(Duration.ofSeconds(30));
        projector.close(Duration.ofSeconds(30));
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public Health health() {
        return (calculator.state() == KafkaStreams.State.RUNNING
                && projector.state() == KafkaStreams.State.RUNNING
                ? Health.up()
                : Health.down())
                .withDetail("calculator", calculator.state())
                .withDetail("projector", projector.state())
                .build();
    }
}
