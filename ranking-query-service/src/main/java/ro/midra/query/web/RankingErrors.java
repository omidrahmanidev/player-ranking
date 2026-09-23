package ro.midra.query.web;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import ro.midra.query.application.Rankings.PlayerNotRankedException;
import ro.midra.query.infrastructure.RedisRankingReader.RankingUnavailableException;

@RestControllerAdvice
public class RankingErrors {
    @ExceptionHandler(PlayerNotRankedException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public ProblemDetail absent() {
        return ProblemDetail.forStatusAndDetail(
                HttpStatus.NOT_FOUND, "Player has no active contributions");
    }

    @ExceptionHandler(RankingUnavailableException.class)
    public ResponseEntity<ProblemDetail> unavailable() {
        return ResponseEntity.status(503)
                .header("Retry-After", "1")
                .body(
                        ProblemDetail.forStatusAndDetail(
                                HttpStatus.SERVICE_UNAVAILABLE, "Ranking is rebuilding or not fresh"));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ProblemDetail invalid(IllegalArgumentException error) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, error.getMessage());
    }
}
