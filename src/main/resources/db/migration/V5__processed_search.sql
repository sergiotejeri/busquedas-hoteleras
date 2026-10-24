CREATE TABLE processed_search (
    search_id RAW(16) PRIMARY KEY REFERENCES search_request(search_id),
    group_id RAW(16) NOT NULL REFERENCES criteria_group(group_id)
);

CREATE INDEX processed_search_group_ix ON processed_search(group_id);
