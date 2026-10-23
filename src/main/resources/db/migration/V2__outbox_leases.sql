ALTER TABLE search_outbox ADD (
    lease_token RAW(16),
    available_at TIMESTAMP(9) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
);

CREATE INDEX search_outbox_due_ix ON search_outbox (state, available_at, search_id);
