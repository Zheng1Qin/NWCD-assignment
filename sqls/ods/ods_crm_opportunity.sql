CREATE TABLE ods_crm_opportunity (
    opportunity_id    INTEGER PRIMARY KEY,
    opportunity_name  VARCHAR(255),
    account_id        INTEGER,
    forecast_amount   DECIMAL(14, 2),
    stage             VARCHAR(100),
    create_date       DATE,
    launch_date       DATE
);
