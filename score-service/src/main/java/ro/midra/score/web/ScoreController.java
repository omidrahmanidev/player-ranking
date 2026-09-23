package ro.midra.score.web;

import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import ro.midra.score.application.AcceptScore;
import ro.midra.shared.ScoreEvent;

import java.time.Instant;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/scores")
public class ScoreController {
    private final AcceptScore acceptScore;

    public ScoreController(AcceptScore acceptScore) {
        this.acceptScore = acceptScore;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ScoreEvent accept(@Valid @RequestBody Request request) {
        return acceptScore.accept(
                request.eventId(), request.playerId(), request.score(), request.eventTime());
    }

    public record Request(
            UUID eventId,
            @NotBlank
            @Pattern(regexp = "[a-zA-Z0-9_-]{1,64}")
            String playerId,

            @Min(1)
            @Max(1000000)
            long score,

            @NotNull
            Instant eventTime) {
    }
}
