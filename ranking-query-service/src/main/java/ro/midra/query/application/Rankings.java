package ro.midra.query.application;

import org.springframework.stereotype.Service;
import ro.midra.query.domain.RankingResult;

@Service
public class Rankings {
    private final RankingReader reader;

    public Rankings(RankingReader reader) {
        this.reader = reader;
    }

    public RankingResult top() {
        return reader.read("top", "");
    }

    public RankingResult rank(String player) {
        return player("rank", player);
    }

    public RankingResult neighbors(String player) {
        return player("neighbors", player);
    }

    private RankingResult player(String mode, String player) {
        if (!player.matches("[a-zA-Z0-9_-]{1,64}"))
            throw new IllegalArgumentException("Invalid playerId");
        RankingResult result = reader.read(mode, player);
        if (result.players().isEmpty()) throw new PlayerNotRankedException();
        return result;
    }

    public static class PlayerNotRankedException extends RuntimeException {
    }
}
