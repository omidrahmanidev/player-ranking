package ro.midra.query.application;

import ro.midra.query.domain.RankingResult;

/**
 * A read either returns a complete projection or fails as unavailable during recovery.
 */
public interface RankingReader {
    RankingResult read(String mode, String playerId);
}
