CREATE TABLE dm_forecast_weekly_revenue (
    year INT,
    week INT,
    forecast_weekly_revenue DECIMAL(14, 2),
    partition_date DATE,
    primary key (partition_date, year, week)
);
