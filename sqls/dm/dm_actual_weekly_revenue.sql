CREATE TABLE dm_actual_weekly_revenue (
    year INT,
    week INT,
    actual_weekly_revenue DECIMAL(14, 2),
    partition_date DATE,
    primary key (partition_date, year, week)
);
