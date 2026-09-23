package ro.midra.stream.domain;

/**
 * A zero contribution count removes the player, rather than leaving a permanent zero-score member.
 */
public record PlayerTotal(long score, long contributions) {
    private static final long MAX_EXACT_REDIS_INTEGER = 9_007_199_254_740_991L;

    public PlayerTotal add(long delta) {
        long next = Math.addExact(score, delta);
        if (next > MAX_EXACT_REDIS_INTEGER)
            throw new IllegalArgumentException("Player score exceeds exact Redis integer range");
        return new PlayerTotal(next, contributions + 1);
    }

    public PlayerTotal remove(long delta) {
        return new PlayerTotal(Math.subtractExact(score, delta), contributions - 1);
    }
}
