CREATE TABLE search_request (
    search_id RAW(16) PRIMARY KEY,
    criteria_json CLOB NOT NULL,
    status VARCHAR2(16 CHAR) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE,
    CONSTRAINT search_request_status_ck CHECK (status IN ('PENDING', 'PROCESSED', 'FAILED'))
);

CREATE TABLE search_outbox (
    search_id RAW(16) PRIMARY KEY,
    event_json CLOB NOT NULL,
    state VARCHAR2(16 CHAR) DEFAULT 'READY' NOT NULL,
    CONSTRAINT search_outbox_request_fk FOREIGN KEY (search_id) REFERENCES search_request(search_id),
    CONSTRAINT search_outbox_state_ck CHECK (state IN ('READY', 'SENT', 'FAILED'))
);
