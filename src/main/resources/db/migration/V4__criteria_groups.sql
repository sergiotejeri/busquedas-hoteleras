CREATE TABLE criteria_group_lock (
    criteria_hash RAW(32) PRIMARY KEY
);

CREATE TABLE criteria_group (
    group_id RAW(16) PRIMARY KEY,
    criteria_hash RAW(32) NOT NULL,
    criteria_json CLOB NOT NULL,
    CONSTRAINT criteria_group_hash_fk FOREIGN KEY (criteria_hash)
        REFERENCES criteria_group_lock(criteria_hash)
);

CREATE INDEX criteria_group_hash_ix ON criteria_group(criteria_hash);
