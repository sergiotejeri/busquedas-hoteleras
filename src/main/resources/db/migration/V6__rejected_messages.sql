CREATE TABLE rejected_message_receipt (
    topic_name VARCHAR2(249 CHAR) NOT NULL,
    partition_no NUMBER(10) NOT NULL CHECK (partition_no >= 0),
    offset_no NUMBER(19) NOT NULL CHECK (offset_no >= 0),
    CONSTRAINT rejected_message_receipt_pk PRIMARY KEY (topic_name, partition_no, offset_no)
);

CREATE TABLE rejected_message (
    topic_name VARCHAR2(249 CHAR) NOT NULL,
    partition_no NUMBER(10) NOT NULL,
    offset_no NUMBER(19) NOT NULL,
    event_key CLOB,
    raw_payload CLOB,
    reason_code VARCHAR2(80 CHAR) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT rejected_message_pk PRIMARY KEY (topic_name, partition_no, offset_no),
    CONSTRAINT rejected_message_receipt_fk FOREIGN KEY (topic_name, partition_no, offset_no)
        REFERENCES rejected_message_receipt(topic_name, partition_no, offset_no)
);
