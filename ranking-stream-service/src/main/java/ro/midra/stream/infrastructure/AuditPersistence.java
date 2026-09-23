package ro.midra.stream.infrastructure;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ro.midra.shared.Json;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

/**
 * Low-volume completed-minute audit, outside both ingestion and ranking read paths.
 */
@Component
public class AuditPersistence {
    private final JdbcTemplate jdbc;

    public AuditPersistence(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @KafkaListener(
            topics = "${ranking.topics.audit:ranking-audit-v1}",
            groupId = "player-ranking-audit-v1")
    @Transactional
    public void persist(List<String> messages) {
        List<AuditSummary> summaries =
                messages.stream().map(value -> Json.read(value, AuditSummary.class)).toList();
        jdbc.batchUpdate(
                """
                        INSERT INTO score_processing_audit(source_partition, minute, accepted, duplicates, rejected)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT (source_partition, minute) DO UPDATE SET
                          accepted = greatest(score_processing_audit.accepted, excluded.accepted),
                          duplicates = greatest(score_processing_audit.duplicates, excluded.duplicates),
                          rejected = greatest(score_processing_audit.rejected, excluded.rejected)
                        """,
                summaries,
                200,
                (statement, summary) -> {
                    statement.setInt(1, summary.partition());
                    statement.setTimestamp(2, Timestamp.from(Instant.ofEpochMilli(summary.minute())));
                    statement.setLong(3, summary.accepted());
                    statement.setLong(4, summary.duplicate());
                    statement.setLong(5, summary.rejected());
                });
    }
}
