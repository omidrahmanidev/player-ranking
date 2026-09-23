package ro.midra.stream.infrastructure;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import ro.midra.shared.Json;
import ro.midra.shared.ScoreEvent;
import ro.midra.stream.domain.PlayerTotal;
import ro.midra.stream.domain.WindowPolicy;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * One instance per source partition, confined to its Streams thread. Expiry and dedup indexes
 * permit bounded range scans instead of scanning every player. All stores and output records commit
 * together under exactly_once_v2. A clock heartbeat is emitted only after due expiry drains.
 */
public final class WindowProcessor extends ContextualProcessor<String, String, String, String> {
    public static final List<String> STORES =
            List.of("totals", "expiry", "seen", "seen-expiry", "metadata", "audit");
    private static final int SWEEP_LIMIT = 10_000;
    private final Clock clock;
    private final WindowPolicy policy;
    private KeyValueStore<String, String> totals, expiry, seen, seenExpiry, metadata, audit;

    public WindowProcessor(Clock clock, WindowPolicy policy) {
        this.clock = clock;
        this.policy = policy;
    }

    @Override
    public void init(ProcessorContext<String, String> context) {
        super.init(context);
        totals = context.getStateStore("totals");
        expiry = context.getStateStore("expiry");
        seen = context.getStateStore("seen");
        seenExpiry = context.getStateStore("seen-expiry");
        metadata = context.getStateStore("metadata");
        audit = context.getStateStore("audit");
        context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, ignored -> tick());
    }

    @Override
    public void process(Record<String, String> record) {
        long now = time();
        ScoreEvent event = Json.read(record.value(), ScoreEvent.class);
        validate(record.key(), event);
        String identity = event.playerId() + ":" + event.eventId();
        if (seen.get(identity) != null) {
            count(now, "duplicate");
            return;
        }
        if (!policy.accepts(event, now)) {
            count(now, "rejected");
            return;
        }
        addContribution(event, identity, now);
        count(now, "accepted");
    }

    private void validate(String key, ScoreEvent event) {
        if (event.eventId() == null
                || event.eventTime() == null
                || event.receivedAt() == null
                || event.playerId() == null
                || !event.playerId().matches("[a-zA-Z0-9_-]{1,64}")
                || !event.playerId().equals(key)
                || event.score() < 1
                || event.score() > 1_000_000)
            throw new IllegalArgumentException("Invalid trusted score topic contract");
    }

    private void addContribution(ScoreEvent event, String identity, long now) {
        PlayerTotal total = total(event.playerId()).add(event.score());
        totals.put(event.playerId(), Json.write(total));
        expiry.put(StoreAccess.timedKey(policy.expiresAt(event), identity), Json.write(event));
        seen.put(identity, Long.toString(now));
        seenExpiry.put(
                StoreAccess.timedKey(now + policy.dedupRetention().toMillis(), identity), identity);
        publishTotal(event.playerId(), total, now);
    }

    private void tick() {
        long now = time();
        boolean expired = expireContributions(now);
        pruneDedup(now);
        publishAudit(now);
        if (expired)
            forward(
                    "~clock:" + context().taskId().partition(),
                    new RankingUpdate(null, 0, 0, now, context().taskId().partition()),
                    "totals-sink",
                    now);
    }

    private boolean expireContributions(long now) {
        List<KeyValue<String, String>> due = due(expiry, now);
        for (var entry : due) {
            ScoreEvent event = Json.read(entry.value, ScoreEvent.class);
            PlayerTotal total = total(event.playerId()).remove(event.score());
            if (total.contributions() == 0) totals.delete(event.playerId());
            else totals.put(event.playerId(), Json.write(total));
            expiry.delete(entry.key);
            publishTotal(event.playerId(), total, now);
        }
        return due.size() < SWEEP_LIMIT;
    }

    private void pruneDedup(long now) {
        for (var entry : due(seenExpiry, now)) {
            seen.delete(entry.value);
            seenExpiry.delete(entry.key);
        }
    }

    private List<KeyValue<String, String>> due(KeyValueStore<String, String> store, long now) {
        var entries = new ArrayList<KeyValue<String, String>>();
        try (var iterator = store.range("", StoreAccess.timedKey(now, "~"))) {
            while (iterator.hasNext() && entries.size() < SWEEP_LIMIT) entries.add(iterator.next());
        }
        return entries;
    }

    private long time() {
        String previous = metadata.get("clock");
        long now = Math.max(clock.millis(), previous == null ? 0 : Long.parseLong(previous));
        metadata.put("clock", Long.toString(now));
        return now;
    }

    private PlayerTotal total(String player) {
        return StoreAccess.read(totals, player, PlayerTotal.class, new PlayerTotal(0, 0));
    }

    private void publishTotal(String player, PlayerTotal total, long now) {
        forward(
                player,
                new RankingUpdate(
                        player, total.score(), total.contributions(), now, context().taskId().partition()),
                "totals-sink",
                now);
    }

    private void count(long now, String outcome) {
        long minute = now / 60_000 * 60_000;
        String key = StoreAccess.timedKey(minute, "count");
        var current =
                StoreAccess.read(
                        audit,
                        key,
                        AuditSummary.class,
                        new AuditSummary(context().taskId().partition(), minute, 0, 0, 0));
        var next =
                new AuditSummary(
                        current.partition(),
                        minute,
                        current.accepted() + (outcome.equals("accepted") ? 1 : 0),
                        current.duplicate() + (outcome.equals("duplicate") ? 1 : 0),
                        current.rejected() + (outcome.equals("rejected") ? 1 : 0));
        audit.put(key, Json.write(next));
    }

    private void publishAudit(long now) {
        var remove = new ArrayList<String>();
        try (var iterator = audit.all()) {
            while (iterator.hasNext()) {
                var entry = iterator.next();
                var summary = Json.read(entry.value, AuditSummary.class);
                if (summary.minute() + 60_000 <= now) {
                    forward(summary.partition() + ":" + summary.minute(), summary, "audit-sink", now);
                    remove.add(entry.key);
                }
            }
        }
        remove.forEach(audit::delete);
    }

    private void forward(String key, Object value, String sink, long now) {
        context().forward(new Record<>(key, Json.write(value), now), sink);
    }
}
