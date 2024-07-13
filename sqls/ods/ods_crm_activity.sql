CREATE TABLE ods_crm_activity (
    activity_id       INTEGER PRIMARY KEY,
    activity_name     VARCHAR(255),
    activity_type     VARCHAR(100),
    account_id        INTEGER,
    opportunity_id    INTEGER
);
