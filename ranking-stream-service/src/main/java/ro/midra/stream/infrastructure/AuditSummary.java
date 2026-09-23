package ro.midra.stream.infrastructure;

/**
 * Cumulative counts per processing minute and source partition, safe for idempotent SQL upserts.
 */
public record AuditSummary(
        int partition, long minute, long accepted, long duplicate, long rejected) {
}
