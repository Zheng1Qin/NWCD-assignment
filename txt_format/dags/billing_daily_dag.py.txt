import datetime

from airflow import DAG

from airflow.operators.python import PythonOperator

from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from data_process.load_csv_to_postgres import load_csv_to_postgres

from data_process import dw_tables_sqls as dw_sqls

default_args = {"owner": "airflow"}
connection_id = 'dwh'
default_end_time = '2999-12-31 23:59:59'

with DAG(
        dag_id="billing_daily_dag",
        start_date=datetime.datetime(2024, 7, 15),
        schedule_interval='0 18 * * *',  # Runs at 18:00 UTC, which is 02:00 CST (UTC+8)
        default_args=default_args,
        catchup=False,
) as dag:
    dag_start = BashOperator(
        task_id='daily_dag_start',
        bash_command='echo Load raw data to ods',
    )

    load_billing_detail_to_ods = PythonOperator(
        task_id='load_sales_order_to_ods',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'source': "/data/landing/{{ ds }}/billing.csv",
            'target': "/data/raw/{{ ds }}/billing.csv",
            'table_name': 'ods.ods_billing_detail',
            'connection_id': connection_id,
        },
    )

    ods_finished = BashOperator(
        task_id='ods_finished',
        bash_command='echo All data have been loaded to ods',
    )

    dw_billing_detail = PostgresOperator(
        task_id="dw_product_description",
        postgres_conn_id=connection_id,
        sql=dw_sqls.dw_billing_detail_sql,
    )

    dw_finished = BashOperator(
        task_id='dw_finished',
        bash_command='echo All data have been transformed from ods to dw',
    )

    dag_start >> load_billing_detail_to_ods >> ods_finished >> dw_finished >> dw_finished
