CREATE TABLE score_processing_audit (
    source_partition integer NOT NULL,
    minute timestamptz NOT NULL,
    accepted bigint NOT NULL CHECK (accepted >= 0),
    duplicates bigint NOT NULL CHECK (duplicates >= 0),
    rejected bigint NOT NULL CHECK (rejected >= 0),
    PRIMARY KEY (source_partition, minute)
);
CREATE INDEX score_processing_audit_minute_idx ON score_processing_audit (minute);
