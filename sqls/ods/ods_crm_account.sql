CREATE TABLE ods_crm_account (
    account_id          INTEGER PRIMARY KEY,
    account_name        VARCHAR(255),
    owner_territory_id  INTEGER,
    active_flag         BOOLEAN,
    forecast_amount     DECIMAL(14, 2)
);
