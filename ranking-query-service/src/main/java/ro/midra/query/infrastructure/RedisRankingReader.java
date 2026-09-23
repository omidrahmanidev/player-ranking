package ro.midra.query.infrastructure;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;
import ro.midra.query.application.RankingReader;
import ro.midra.query.domain.RankingEntry;
import ro.midra.query.domain.RankingResult;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * One Lua call supplies a consistent cross-partition snapshot without read-side fan-out races.
 */
@Component
public class RedisRankingReader implements RankingReader {
    private final StringRedisTemplate redis;
    private final List<String> keys;
    private final DefaultRedisScript<List> script;

    public RedisRankingReader(
            StringRedisTemplate redis, @Value("${ranking.partitions:32}") int partitions) {
        if (partitions < 1) throw new IllegalArgumentException("ranking.partitions must be positive");
        this.redis = redis;
        this.keys = new ArrayList<>();
        for (int partition = 0; partition < partitions; partition++) {
            keys.add("player-ranking:10m:" + partition);
            keys.add("player-ranking:10m:" + partition + ":ready");
            keys.add("player-ranking:10m:" + partition + ":owner");
        }
        script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("redis/ranking.lua"));
        script.setResultType(List.class);
    }

    @Override
    public RankingResult read(String mode, String playerId) {
        List<?> result;
        try {
            result = redis.execute(script, keys, mode, playerId);
        } catch (RuntimeException error) {
            throw new RankingUnavailableException(error);
        }
        if (result == null || result.isEmpty()) throw new RankingUnavailableException(null);
        var entries = new ArrayList<RankingEntry>();
        for (int i = 2; i < result.size(); i += 3)
            entries.add(
                    new RankingEntry(
                            result.get(i).toString(), number(result.get(i + 1)), number(result.get(i + 2))));
        return new RankingResult(
                Instant.ofEpochMilli(number(result.getFirst())), number(result.get(1)), entries);
    }

    private long number(Object value) {
        return Long.parseLong(value.toString());
    }

    public static class RankingUnavailableException extends RuntimeException {
        public RankingUnavailableException(Throwable cause) {
            super("Ranking is rebuilding or not fresh", cause);
        }
    }
}
