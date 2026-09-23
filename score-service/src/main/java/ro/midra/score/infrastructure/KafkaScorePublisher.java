package ro.midra.score.infrastructure;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import ro.midra.score.application.ScorePublisher;
import ro.midra.shared.Json;
import ro.midra.shared.ScoreEvent;

import java.util.concurrent.TimeUnit;

@Component
public class KafkaScorePublisher implements ScorePublisher {
    private final KafkaTemplate<String, String> kafka;
    private final String topic;

    public KafkaScorePublisher(
            KafkaTemplate<String, String> kafka,
            @Value("${ranking.topics.scores:score-events-v1}") String topic) {
        this.kafka = kafka;
        this.topic = topic;
    }

    @Override
    public void publish(ScoreEvent event) {
        try {
            kafka.send(topic, event.playerId(), Json.write(event)).get(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PublishUnavailableException(e);
        } catch (Exception e) {
            throw new PublishUnavailableException(e);
        }
    }

    public static class PublishUnavailableException extends RuntimeException {
        public PublishUnavailableException(Throwable cause) {
            super("Score publication unavailable; retry with the same eventId", cause);
        }
    }
}
