package ro.midra.score.application;

import org.springframework.stereotype.Service;
import ro.midra.shared.ScoreEvent;

import java.time.Clock;
import java.time.Instant;
import java.util.UUID;

/**
 * Assigns transport metadata without interpreting or calculating ranking scores.
 */
@Service
public class AcceptScore {
    private final ScorePublisher publisher;
    private final Clock clock;

    public AcceptScore(ScorePublisher publisher, Clock clock) {
        this.publisher = publisher;
        this.clock = clock;
    }

    public ScoreEvent accept(UUID eventId, String playerId, long score, Instant eventTime) {
        Instant now = clock.instant();
        if (eventTime.isBefore(Instant.EPOCH))
            throw new IllegalArgumentException("eventTime must be on or after the Unix epoch");
        if (eventTime.isAfter(now))
            throw new IllegalArgumentException("eventTime must not be in the future");
        var event =
                new ScoreEvent(
                        eventId == null ? UUID.randomUUID() : eventId, playerId, score, eventTime, now);
        publisher.publish(event);
        return event;
    }
}
