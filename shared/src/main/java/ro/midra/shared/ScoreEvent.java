package ro.midra.shared;

import java.time.Instant;
import java.util.UUID;

/**
 * Wire contract. Identity is (playerId, eventId); retries must preserve the entire payload.
 */
public record ScoreEvent(
        UUID eventId, String playerId, long score, Instant eventTime, Instant receivedAt) {
}
