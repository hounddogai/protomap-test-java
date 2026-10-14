CREATE TABLE IF NOT EXISTS finance_records (
    tenant_id VARCHAR(200) NOT NULL,
    entity_kind VARCHAR(200) NOT NULL,
    record_id VARCHAR(200) NOT NULL,
    label TEXT NOT NULL,
    snapshot BYTEA NOT NULL,
    PRIMARY KEY (tenant_id, entity_kind, record_id)
);
CREATE TABLE IF NOT EXISTS finance_actions (
    tenant_id VARCHAR(200) NOT NULL,
    entity_kind VARCHAR(200) NOT NULL,
    record_id VARCHAR(200) NOT NULL,
    operation VARCHAR(200) NOT NULL
);
CREATE TABLE IF NOT EXISTS finance_details (
    tenant_id VARCHAR(200) NOT NULL,
    operation VARCHAR(200) NOT NULL,
    detail TEXT NOT NULL
);
