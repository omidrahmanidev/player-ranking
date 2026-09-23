package ro.midra.integration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import ro.midra.query.domain.RankingEntry;
import ro.midra.query.infrastructure.RedisRankingReader;
import ro.midra.query.infrastructure.RedisRankingReader.RankingUnavailableException;

import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Random;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
class RedisQueriesIT {
    private static final int PARTITIONS = 8;

    @Container
    static final GenericContainer<?> REDIS =
            new GenericContainer<>("redis:7.4.6-alpine").withExposedPorts(6379);

    private static LettuceConnectionFactory connection;
    private StringRedisTemplate redis;
    private RedisRankingReader reader;

    @BeforeAll
    static void connect() {
        connection = new LettuceConnectionFactory(REDIS.getHost(), REDIS.getMappedPort(6379));
        connection.afterPropertiesSet();
    }

    @AfterAll
    static void close() {
        connection.destroy();
    }

    @BeforeEach
    void prepare() {
        redis = new StringRedisTemplate(connection);
        try (var client = connection.getConnection()) {
            client.serverCommands().flushAll();
        }
        reader = new RedisRankingReader(redis, PARTITIONS);
        ready();
    }

    @Test
    void ranksAndNeighborsMatchFullSortAcrossLargeTieGroups() {
        var random = new Random(17);
        var scores = new HashMap<String, Long>();
        for (int i = 0; i < 250; i++) {
            String player = "player-%04d".formatted(i);
            long score = random.nextInt(5);
            scores.put(player, score);
            redis.opsForZSet().add(key(random.nextInt(PARTITIONS)), player, score);
        }
        var order =
                scores.keySet().stream()
                        .sorted(
                                Comparator.<String>comparingLong(scores::get)
                                        .reversed()
                                        .thenComparing(Comparator.reverseOrder()))
                        .toList();
        var expected =
                IntStream.range(0, order.size())
                        .mapToObj(i -> new RankingEntry(order.get(i), scores.get(order.get(i)), i + 1))
                        .toList();
        assertThat(reader.read("top", "").players()).containsExactlyElementsOf(expected.subList(0, 10));
        for (int i = 0; i < order.size(); i++) {
            ready();
            assertThat(reader.read("rank", order.get(i)).players()).containsExactly(expected.get(i));
            assertThat(reader.read("neighbors", order.get(i)).players())
                    .containsExactlyElementsOf(
                            expected.subList(Math.max(0, i - 2), Math.min(order.size(), i + 3)));
        }
        assertThat(reader.read("top", "").totalPlayers()).isEqualTo(250);
    }

    @Test
    void emptyRankingAndMissingPlayersAreEmptyResults() {
        assertThat(reader.read("top", "").players()).isEmpty();
        assertThat(reader.read("rank", "absent").players()).isEmpty();
        assertThat(reader.read("neighbors", "absent").players()).isEmpty();
    }

    @Test
    void anyUnreadyOrStalePartitionPreventsPartialRead() {
        redis.opsForZSet().add(key(0), "a", 10);
        redis.delete(key(7) + ":ready");
        assertThatThrownBy(() -> reader.read("top", ""))
                .isInstanceOf(RankingUnavailableException.class);
        ready();
        redis.opsForValue().set(key(2) + ":ready", Long.toString(System.currentTimeMillis() - 6000));
        assertThatThrownBy(() -> reader.read("rank", "a"))
                .isInstanceOf(RankingUnavailableException.class);
        ready();
        redis.delete(key(4) + ":owner");
        assertThatThrownBy(() -> reader.read("neighbors", "a"))
                .isInstanceOf(RankingUnavailableException.class);
    }

    @Test
    void exactIntegerLimitRoundTripsWithoutRounding() {
        long maximum = 9_007_199_254_740_991L;
        redis.opsForZSet().add(key(0), "maximum", maximum);
        assertThat(reader.read("rank", "maximum").players().getFirst().score()).isEqualTo(maximum);
        assertThat(reader.read("top", "").players().getFirst().score()).isEqualTo(maximum);
    }

    private void ready() {
        for (int partition = 0; partition < PARTITIONS; partition++) {
            redis.opsForValue().set(key(partition) + ":owner", "test", Duration.ofSeconds(30));
            redis
                    .opsForValue()
                    .set(
                            key(partition) + ":ready",
                            Long.toString(System.currentTimeMillis()),
                            Duration.ofSeconds(30));
        }
    }

    private String key(int partition) {
        return "player-ranking:10m:" + partition;
    }
}
