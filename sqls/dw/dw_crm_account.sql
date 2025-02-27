CREATE TABLE dw_crm_account (
    account_id          INTEGER,
    account_name        VARCHAR(255),
    owner_territory_id  INTEGER,
    active_flag         BOOLEAN,
    forecast_amount     DECIMAL(14, 2),
    partition_date      DATE,
    primary key (account_id, partition_date)
);
