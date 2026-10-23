ALTER TABLE search_outbox ADD (
    retry_attempts NUMBER(2) DEFAULT 0 NOT NULL,
    CONSTRAINT search_outbox_retries_ck CHECK (retry_attempts BETWEEN 0 AND 6)
);
