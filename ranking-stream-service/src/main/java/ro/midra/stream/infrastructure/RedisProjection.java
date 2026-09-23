package ro.midra.stream.infrastructure;

import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Each task owns a leased token. Every publication checks that token, fencing paused or revoked
 * tasks. Rebuilds stage a complete state-store snapshot before an atomic rename. Redis is
 * disposable.
 */
public final class RedisProjection {
    private static final Logger LOG = LoggerFactory.getLogger(RedisProjection.class);
    private static final long LEASE_MS = 15_000;
    private static final DefaultRedisScript<Long> RENEW =
            script(
                    """
                            if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
                            redis.call('PEXPIRE', KEYS[1], ARGV[2])
                            return 1
                            """);
    private static final DefaultRedisScript<Long> UPDATE =
            script(
                    """
                            if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
                            for i=2,#ARGV,2 do
                              if ARGV[i+1] == '-1' then redis.call('ZREM', KEYS[2], ARGV[i])
                              else redis.call('ZADD', KEYS[2], ARGV[i+1], ARGV[i]) end
                            end
                            return 1
                            """);
    private static final DefaultRedisScript<Long> PUBLISH =
            script(
                    """
                            if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
                            redis.call('DEL', KEYS[2])
                            if redis.call('EXISTS', KEYS[3]) == 1 then
                              redis.call('RENAME', KEYS[3], KEYS[2])
                              redis.call('PERSIST', KEYS[2])
                            end
                            redis.call('SET', KEYS[4], ARGV[2], 'PX', 5000)
                            return 1
                            """);
    private static final DefaultRedisScript<Long> READY =
            script(
                    """
                            if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
                            redis.call('SET', KEYS[2], ARGV[2], 'PX', 5000)
                            return 1
                            """);
    private static final DefaultRedisScript<Long> RELEASE =
            script(
                    """
                            if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
                            redis.call('DEL', KEYS[1], KEYS[2])
                            return 1
                            """);
    private final StringRedisTemplate redis;
    private final String base;
    private final String token = UUID.randomUUID().toString();
    private boolean owned;

    public RedisProjection(StringRedisTemplate redis, int partition) {
        this.redis = redis;
        this.base = "player-ranking:10m:" + partition;
    }

    public void synchronize(
            KeyValueStore<String, String> state, Map<String, RankingUpdate> dirty, long asOf) {
        if (!renew()) {
            owned = false;
            if (!acquire()) {
                dirty.clear();
                return;
            }
            rebuild(state, asOf);
        } else {
            apply(dirty);
            require(redis.execute(READY, List.of(ownerKey(), readyKey()), token, Long.toString(asOf)));
        }
        dirty.clear();
    }

    private boolean acquire() {
        boolean acquired =
                Boolean.TRUE.equals(
                        redis
                                .opsForValue()
                                .setIfAbsent(ownerKey(), token, java.time.Duration.ofMillis(LEASE_MS)));
        if (acquired) {
            redis.delete(readyKey());
            owned = true;
        }
        return acquired;
    }

    private boolean renew() {
        return owned
                && Long.valueOf(1)
                .equals(redis.execute(RENEW, List.of(ownerKey()), token, Long.toString(LEASE_MS)));
    }

    private void rebuild(KeyValueStore<String, String> state, long asOf) {
        LOG.info("Rebuilding Redis partition {}", base);
        String staging = base + ":building:" + token;
        redis.delete(staging);
        var batch = new ArrayList<RankingUpdate>(1000);
        try (var entries = state.all()) {
            while (entries.hasNext()) {
                batch.add(ro.midra.shared.Json.read(entries.next().value, RankingUpdate.class));
                if (batch.size() == 1000) stage(staging, batch);
            }
        }
        stage(staging, batch);
        require(
                redis.execute(
                        PUBLISH, List.of(ownerKey(), base, staging, readyKey()), token, Long.toString(asOf)));
        LOG.info("Published Redis partition {}", base);
    }

    private void stage(String staging, List<RankingUpdate> batch) {
        if (!renew()) throw new IllegalStateException("Projection ownership lost during rebuild");
        if (batch.isEmpty()) return;
        redis.executePipelined(
                (RedisCallback<Object>)
                        connection -> {
                            for (var update : batch)
                                connection
                                        .zSetCommands()
                                        .zAdd(bytes(staging), update.score(), bytes(update.playerId()));
                            connection.keyCommands().pExpire(bytes(staging), 60_000);
                            return null;
                        });
        batch.clear();
    }

    private void apply(Map<String, RankingUpdate> dirty) {
        var arguments = new ArrayList<String>();
        arguments.add(token);
        for (var update : dirty.values()) {
            arguments.add(update.playerId());
            arguments.add(update.contributions() == 0 ? "-1" : Long.toString(update.score()));
            if (arguments.size() >= 2001) flush(arguments);
        }
        if (arguments.size() > 1) flush(arguments);
    }

    private void flush(List<String> arguments) {
        require(redis.execute(UPDATE, List.of(ownerKey(), base), arguments.toArray()));
        arguments.clear();
        arguments.add(token);
    }

    public void close() {
        try {
            redis.execute(RELEASE, List.of(ownerKey(), readyKey()), token);
        } catch (RuntimeException error) {
            LOG.warn("Projection lease will expire for {}", base);
        }
    }

    private String ownerKey() {
        return base + ":owner";
    }

    private String readyKey() {
        return base + ":ready";
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static DefaultRedisScript<Long> script(String text) {
        return new DefaultRedisScript<>(text, Long.class);
    }

    private static void require(Long result) {
        if (!Long.valueOf(1).equals(result))
            throw new IllegalStateException("Projection ownership lost");
    }
}
