CREATE TABLE dw_crm_opportunity (
    opportunity_id    INTEGER,
    opportunity_name  VARCHAR(255),
    account_id        INTEGER,
    forecast_amount   DECIMAL(14, 2),
    stage             VARCHAR(100),
    create_date       DATE,
    launch_date       DATE,
    partition_date      DATE,
    primary key (opportunity_id, partition_date)
);
