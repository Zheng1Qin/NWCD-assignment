CREATE TABLE dm_forecast_vs_actual_weekly_revenue (
    year INT,
    week INT,
    actual_weekly_revenue DECIMAL(14, 2),
    forecast_weekly_revenue DECIMAL(14, 2),
    weekly_revenue_difference DECIMAL(14, 2),
    partition_date DATE,
    primary key (partition_date, year, week)
);
