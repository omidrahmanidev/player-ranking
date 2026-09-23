package ro.midra.query.domain;

/**
 * Ordinal ranks start at one; equal scores are ordered by descending ASCII player ID.
 */
public record RankingEntry(String playerId, long score, long rank) {
}
