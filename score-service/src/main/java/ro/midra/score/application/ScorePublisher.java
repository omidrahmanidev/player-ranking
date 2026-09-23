package ro.midra.score.application;

import ro.midra.shared.ScoreEvent;

/**
 * Returns only after durable broker acknowledgement; a timeout may have an ambiguous outcome.
 */
public interface ScorePublisher {
    void publish(ScoreEvent event);
}
