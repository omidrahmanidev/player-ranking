package ro.midra.stream.infrastructure;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ro.midra.shared.Json;
import ro.midra.shared.ScoreEvent;
import ro.midra.stream.domain.PlayerTotal;
import ro.midra.stream.domain.WindowPolicy;

import java.time.*;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class WindowProcessorTest {
    private final MutableClock clock = new MutableClock(Instant.parse("2026-01-01T14:00:00Z"));
    private TopologyTestDriver driver;
    private TestInputTopic<String, String> input;
    private TestOutputTopic<String, String> output;

    @BeforeEach
    void open() {
        var properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "window-test");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        driver =
                new TopologyTestDriver(
                        RankingTopology.build(
                                clock, WindowPolicy.standard(), "scores", "totals-out", "audit-out"),
                        properties,
                        clock.instant());
        input = driver.createInputTopic("scores", new StringSerializer(), new StringSerializer());
        output =
                driver.createOutputTopic("totals-out", new StringDeserializer(), new StringDeserializer());
    }

    @AfterEach
    void close() {
        driver.close();
    }

    @Test
    void exactTenMinuteWindowExpiresWithoutNewEvents() {
        send(event("a", 50, clock.instant()));
        advance(Duration.ofMinutes(4));
        send(event("a", 30, clock.instant()));
        advance(Duration.ofMinutes(4));
        send(event("a", 20, clock.instant()));
        advance(Duration.ofMinutes(1));
        assertThat(score("a")).isEqualTo(100);
        advance(Duration.ofMinutes(2));
        assertThat(score("a")).isEqualTo(50);
        advance(Duration.ofMinutes(7));
        assertThat(driver.getKeyValueStore("totals").get("a")).isNull();
        assertThat(lastPlayerUpdate().contributions()).isZero();
    }

    @Test
    void lowerWindowBoundaryIsExclusive() {
        send(event("a", 50, clock.instant()));
        advance(Duration.ofMinutes(10).minusMillis(1));
        assertThat(score("a")).isEqualTo(50);
        advance(Duration.ofMillis(1));
        assertThat(driver.getKeyValueStore("totals").get("a")).isNull();
    }

    @Test
    void duplicateDoesNotAddOrRefreshExpiration() {
        var event = event("a", 50, clock.instant());
        send(event);
        advance(Duration.ofSeconds(60));
        send(event);
        assertThat(score("a")).isEqualTo(50);
        advance(Duration.ofMinutes(9));
        assertThat(driver.getKeyValueStore("totals").get("a")).isNull();
    }

    @Test
    void acceptsOutOfOrderAndInclusiveLatenessBoundary() {
        send(event("a", 10, clock.instant()));
        send(event("a", 20, clock.instant().minusSeconds(30)));
        send(event("a", 30, clock.instant().minusSeconds(120)));
        assertThat(score("a")).isEqualTo(60);
    }

    @Test
    void rejectsTooLateAndFutureEvents() {
        send(event("a", 10, clock.instant().minusMillis(120001)));
        send(event("b", 10, clock.instant().plusMillis(1)));
        assertThat(driver.getKeyValueStore("totals").approximateNumEntries()).isZero();
    }

    @Test
    void dedupStateIsBoundedAndOldRetryCannotReenterWindow() {
        var event = event("a", 50, clock.instant());
        send(event);
        advance(Duration.ofMinutes(16));
        assertThat(driver.getKeyValueStore("seen").approximateNumEntries()).isZero();
        assertThat(driver.getKeyValueStore("seen-expiry").approximateNumEntries()).isZero();
        send(event);
        assertThat(driver.getKeyValueStore("totals").approximateNumEntries()).isZero();
    }

    @Test
    void clockRollbackCannotExtendWindow() {
        send(event("a", 50, clock.instant()));
        advance(Duration.ofMinutes(10));
        clock.now = clock.now.minusSeconds(600);
        send(event("a", 50, clock.instant()));
        assertThat(driver.getKeyValueStore("totals").approximateNumEntries()).isZero();
    }

    @Test
    void auditCountsAcceptedDuplicateAndRejected() {
        var event = event("a", 50, clock.instant());
        send(event);
        send(event);
        send(event("b", 10, clock.instant().minusSeconds(121)));
        advance(Duration.ofMinutes(1));
        var audit =
                driver.createOutputTopic("audit-out", new StringDeserializer(), new StringDeserializer());
        var summary = Json.read(audit.readValue(), AuditSummary.class);
        assertThat(summary.accepted()).isEqualTo(1);
        assertThat(summary.duplicate()).isEqualTo(1);
        assertThat(summary.rejected()).isEqualTo(1);
    }

    private long score(String player) {
        return Json.read((String) driver.getKeyValueStore("totals").get(player), PlayerTotal.class)
                .score();
    }

    private RankingUpdate lastPlayerUpdate() {
        return output.readValuesToList().stream()
                .map(value -> Json.read(value, RankingUpdate.class))
                .filter(value -> !value.heartbeat())
                .reduce((a, b) -> b)
                .orElseThrow();
    }

    private ScoreEvent event(String player, long score, Instant timestamp) {
        return new ScoreEvent(UUID.randomUUID(), player, score, timestamp, clock.instant());
    }

    private void send(ScoreEvent event) {
        input.pipeInput(event.playerId(), Json.write(event), event.eventTime());
    }

    private void advance(Duration duration) {
        clock.now = clock.now.plus(duration);
        driver.advanceWallClockTime(Duration.ofSeconds(1));
    }

    static class MutableClock extends Clock {
        Instant now;

        MutableClock(Instant now) {
            this.now = now;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}
