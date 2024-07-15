CREATE TABLE dw_billing_detail (
    account_id         INTEGER,
    sub_account_id     INTEGER,
    owner_territory_id INTEGER,
    product_name       VARCHAR(255),
    charge_type        VARCHAR(100),
    revenue_amount     DECIMAL(14, 2),
    charge_date        DATE
);
