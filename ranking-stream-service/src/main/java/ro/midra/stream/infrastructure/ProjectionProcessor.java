package ro.midra.stream.infrastructure;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import ro.midra.shared.Json;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Consumes only committed calculator output. Persistent current totals are sufficient to rebuild
 * Redis after total loss; the in-memory dirty map only coalesces writes and is never recovery
 * state.
 */
public final class ProjectionProcessor extends ContextualProcessor<String, String, Void, Void> {
    private static final Logger LOG = LoggerFactory.getLogger(ProjectionProcessor.class);
    private final StringRedisTemplate redis;
    private final MeterRegistry metrics;
    private final Map<String, RankingUpdate> dirty = new HashMap<>();
    private KeyValueStore<String, String> state, progress;
    private RedisProjection projection;

    public ProjectionProcessor(StringRedisTemplate redis, MeterRegistry metrics) {
        this.redis = redis;
        this.metrics = metrics;
    }

    @Override
    public void init(ProcessorContext<Void, Void> context) {
        super.init(context);
        state = context.getStateStore("projection");
        progress = context.getStateStore("progress");
        projection = new RedisProjection(redis, context.taskId().partition());
        context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, ignored -> publish());
    }

    @Override
    public void process(Record<String, String> record) {
        var update = Json.read(record.value(), RankingUpdate.class);
        if (update.partition() != context().taskId().partition())
            throw new IllegalArgumentException("Partition contract changed");
        if (update.heartbeat()) {
            progress.put("asOf", Long.toString(update.asOf()));
            return;
        }
        if (update.contributions() == 0) state.delete(update.playerId());
        else state.put(update.playerId(), record.value());
        dirty.put(update.playerId(), update);
        if (dirty.size() >= 10_000) publish();
    }

    private void publish() {
        String asOf = progress.get("asOf");
        if (asOf == null) return;
        try {
            projection.synchronize(state, dirty, Long.parseLong(asOf));
        } catch (RuntimeException error) {
            metrics.counter("ranking.projection.failures").increment();
            LOG.warn("Redis publication failed; recovery will retry: {}", error.toString());
            dirty.clear();
            projection.close();
            projection = new RedisProjection(redis, context().taskId().partition());
        }
    }

    @Override
    public void close() {
        if (projection != null) projection.close();
    }
}
