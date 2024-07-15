CREATE TABLE ods_crm_activity (
    activity_id       INTEGER,
    activity_name     VARCHAR(255),
    activity_type     VARCHAR(100),
    account_id        INTEGER,
    opportunity_id    INTEGER,
    partition_date      DATE,
    primary key (activity_id, partition_date)
);
