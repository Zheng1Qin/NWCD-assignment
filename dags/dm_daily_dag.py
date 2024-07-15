import datetime

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor

from data_process import dm_table_sqls as dm_sqls

default_args = {"owner": "airflow"}
connection_id = 'dwh'
default_end_time = '2999-12-31 23:59:59'

with (DAG(
        dag_id="crm_daily_dag",
        start_date=datetime.datetime(2024, 7, 15),
        schedule_interval='0 18 * * *',  # Runs at 18:00 UTC, which is 02:00 CST (UTC+8)
        default_args=default_args,
        catchup=False,
) as dag):
    wait_for_upstream_dags = BashOperator(
        task_id='wait_for_upstream_dags',
        bash_command='echo wait for upstream dags complete',
    )

    wait_for_crm = ExternalTaskSensor(
        task_id='wait_for_crm',
        external_dag_id='crm_daily_dag',
        external_task_id=None,
        dag=dag
    )

    wait_for_billing = ExternalTaskSensor(
        task_id='wait_for_billing',
        external_dag_id='billing_daily_dag',
        external_task_id=None,
        dag=dag
    )

    dag_start = BashOperator(
        task_id='daily_dag_start',
        bash_command='echo Load raw data to dm',
    )

    dm_forecast_weekly_revenue = PostgresOperator(
        task_id="dm_forecast_weekly_revenue",
        postgres_conn_id=connection_id,
        sql=dm_sqls.dm_forecast_weekly_revenue_sql,
    )

    dm_actual_weekly_revenue = PostgresOperator(
        task_id="dm_actual_weekly_revenue",
        postgres_conn_id=connection_id,
        sql=dm_sqls.dm_actual_weekly_revenue_sql,
    )

    dm_forecast_vs_actual_weekly_revenue = PostgresOperator(
        task_id="dm_forecast_vs_actual_weekly_revenue",
        postgres_conn_id=connection_id,
        sql=dm_sqls.dm_forecast_vs_actual_weekly_revenue_sql,
    )

    dm_finished = BashOperator(
        task_id='dm_finished',
        bash_command='echo All data have been transformed from dw to dm',
    )

    wait_for_upstream_dags >> [wait_for_crm, wait_for_billing] >> dag_start
    dag_start >> [dm_actual_weekly_revenue,
                  dm_forecast_weekly_revenue] >> dm_forecast_vs_actual_weekly_revenue >> dm_finished
