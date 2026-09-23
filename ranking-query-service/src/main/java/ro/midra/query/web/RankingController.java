package ro.midra.query.web;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ro.midra.query.application.Rankings;
import ro.midra.query.domain.RankingResult;

@RestController
@RequestMapping("/api/v1/rankings")
public class RankingController {
    private final Rankings rankings;

    public RankingController(Rankings rankings) {
        this.rankings = rankings;
    }

    @GetMapping("/top")
    public RankingResult top() {
        return rankings.top();
    }

    @GetMapping("/players/{playerId}/rank")
    public RankingResult rank(@PathVariable String playerId) {
        return rankings.rank(playerId);
    }

    @GetMapping("/players/{playerId}/neighbors")
    public RankingResult neighbors(@PathVariable String playerId) {
        return rankings.neighbors(playerId);
    }
}
