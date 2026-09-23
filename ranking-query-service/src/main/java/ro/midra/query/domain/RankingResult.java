package ro.midra.query.domain;

import java.time.Instant;
import java.util.List;

/**
 * asOf is the oldest calculator heartbeat represented in this atomic Redis read.
 */
public record RankingResult(Instant asOf, long totalPlayers, List<RankingEntry> players) {
    public RankingResult {
        players = List.copyOf(players);
    }
}
