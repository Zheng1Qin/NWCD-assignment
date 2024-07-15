dm_actual_weekly_revenue_sql = """
with dim_dates as (select datum, year, week
from dim_dates where year = EXTRACT(YEAR FROM cast({{ ds }} as date))
    )
select a.year,
       a.week,
       sum(b.revenue_amount) as actual_weekly_revenue,
       {{ ds }} as partition_date
from dm_dates a
         left join dw_billing_detail b on b.charge_date = a.datum
group by year, week
"""
dm_forecast_weekly_revenue_sql = """
with dim_dates as (select datum, year, week
from dim_dates where year = EXTRACT(YEAR FROM cast({{ ds }} as date))
    ), dim_coefficient as (
select factor, coefficient
from dim_forecast_correlation_coefficients
    ), factor_amount as (
SELECT
    o.opportunity_id, o.stage, MAX (CASE WHEN a.activity_type = 'Communications' THEN 1 ELSE 0 END) AS communications, MAX (CASE WHEN a.activity_type = 'Meetings' THEN 1 ELSE 0 END) AS meetings, MAX (CASE WHEN a.activity_type = 'Showcases' THEN 1 ELSE 0 END) AS showcases, MAX (CASE WHEN a.activity_type = 'Site Visits' THEN 1 ELSE 0 END) AS site_visits, o.forecast_amount as forecast_amount, o.launch_date as launch_date
FROM
    (select * from dw_crm_opportunity where partition_date in (select MAX (partition_date) from dw_crm_opportunity)) o
    JOIN
    (select * from dw_crm_activity where partition_date in (select MAX (partition_date) from dw_crm_activity)) a
ON o.opportunity_id = a.opportunity_id
    LEFT JOIN
    (select * from dw_billing_detail where partition_date in (select MAX (partition_date) from dw_billing_detail)) b ON o.account_id = b.account_id
GROUP BY
    o.opportunity_id,
    o.stage,
    b.account_id
    ),
    forecast_amount as (
select
    a.forecast_amount * (b.coefficient + a.communications * (select coefficient from dim_coefficient where factor = 'communications') + a.meetings * (select coefficient from dim_coefficient where factor = 'meetings') + a.showcases * (select coefficient from dim_coefficient where factor = 'showcases') + a.site_visits * (select coefficient from dim_coefficient where factor = 'site_visits') as factor_amount, ) AS forecast_revenue, a.launch_date as launchdate
from factor_amount a
    left join dim_coefficient b
on a.stage = b.factor
    )
select a.year,
       a.week,
       sum(b.forecast_revenue) as forecast_weekly_revenue,
       {{ ds }} as partition_date
from dim_dates a
         left join forecast_amount b on b.launchdate = a.datum
group by a.year, a.week
"""

dm_forecast_vs_actual_weekly_revenue_sql = """
with forecast_weekly_revenue as (
select * from dm_forecast_weekly_revenue where partition_date = {{ ds }}
),
actual_weekly_revenue as (
select * from dm_actual_weekly_revenue where partition_date = {{ ds }}
)
select a.year,
       a.week,
       a.actual_weekly_revenue,
       b.forecast_weekly_revenue,
       a.actual_weekly_revenue - b.forecast_weekly_revenue as weekly_revenue_difference,
       {{ ds }} as partition_date
from forecast_weekly_revenue a
         left join actual_weekly_revenue b on a.year = b.year and a.week = b.week
"""
