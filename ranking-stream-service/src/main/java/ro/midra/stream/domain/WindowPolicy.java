package ro.midra.stream.domain;

import ro.midra.shared.ScoreEvent;

import java.time.Duration;

/**
 * Active membership is (now - window, now]. Event timestamps set expiration; the injected UTC clock
 * advances the window even when no players are scoring.
 */
public record WindowPolicy(Duration window, Duration lateness, Duration dedupRetention) {
    public static WindowPolicy standard() {
        return new WindowPolicy(Duration.ofMinutes(10), Duration.ofMinutes(2), Duration.ofMinutes(15));
    }

    public WindowPolicy {
        if (window.isNegative()
                || window.isZero()
                || lateness.isNegative()
                || lateness.compareTo(window) > 0
                || dedupRetention.compareTo(window.plus(lateness)) < 0)
            throw new IllegalArgumentException("Invalid window retention");
    }

    public boolean accepts(ScoreEvent event, long now) {
        long timestamp = event.eventTime().toEpochMilli();
        return timestamp <= now
                && timestamp >= now - lateness.toMillis()
                && timestamp > now - window.toMillis();
    }

    public long expiresAt(ScoreEvent event) {
        return event.eventTime().toEpochMilli() + window.toMillis();
    }
}
