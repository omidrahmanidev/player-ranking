package ro.midra.stream.infrastructure;

/**
 * Absolute totals make the external projection replayable without distributed transactions.
 */
public record RankingUpdate(
        String playerId, long score, long contributions, long asOf, int partition) {
    public boolean heartbeat() {
        return playerId == null;
    }
}
