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
        dag_id="crm_daily_dag",
        start_date=datetime.datetime(2024, 7, 15),
        schedule_interval='0 18 * * *',  # Runs at 18:00 UTC, which is 02:00 CST (UTC+8)
        default_args=default_args,
        catchup=False,
) as dag:
    dag_start = BashOperator(
        task_id='daily_dag_start',
        bash_command='echo Load raw data to ods',
    )

    load_crm_owner_to_ods = PythonOperator(
        task_id='load_crm_owner_to_ods',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'source': "/data/landing/{{ ds }}/crm_owner.csv",
            'target': "/data/raw/{{ ds }}/crm_owner.csv",
            'table_name': 'ods.ods_crm_owner',
            'connection_id': connection_id,
        },
    )

    load_crm_opportunity_to_ods = PythonOperator(
        task_id='load_crm_opportunity_to_ods',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'source': "/data/landing/{{ ds }}/crm_opportunity.csv",
            'target': "/data/raw/{{ ds }}/crm_opportunity.csv",
            'table_name': 'ods.ods_crm_opportunity',
            'connection_id': connection_id,
        },
    )

    load_crm_activity_to_ods = PythonOperator(
        task_id='load_crm_activity_to_ods',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'source': "/data/landing/{{ ds }}/crm_activity.csv",
            'target': "/data/raw/{{ ds }}/crm_activity.csv",
            'table_name': 'ods.ods_crm_activity',
            'connection_id': connection_id,
        },
    )

    load_crm_account_to_ods = PythonOperator(
        task_id='load_crm_account_to_ods',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'source': "/data/landing/{{ ds }}/crm_account.csv",
            'target': "/data/raw/{{ ds }}/crm_account.csv",
            'table_name': 'ods.ods_crm_account',
            'connection_id': connection_id,
        },
    )

    ods_finished = BashOperator(
        task_id='ods_finished',
        bash_command='echo All data have been loaded to ods',
    )

    dw_crm_account = PostgresOperator(
        task_id="dw_crm_account",
        postgres_conn_id=connection_id,
        sql=dw_sqls.dw_crm_account_sql,
    )
    dw_crm_activity = PostgresOperator(
        task_id="dw_crm_activity",
        postgres_conn_id=connection_id,
        sql=dw_sqls.dw_crm_activity_sql,
    )
    dw_crm_opportunity = PostgresOperator(
        task_id="dw_crm_opportunity",
        postgres_conn_id=connection_id,
        sql=dw_sqls.dw_crm_opportunity_sql,
    )
    dw_crm_owner = PostgresOperator(
        task_id="dw_crm_owner",
        postgres_conn_id=connection_id,
        sql=dw_sqls.dw_crm_owner_sql,
    )
    dw_finished = BashOperator(
        task_id='dw_finished',
        bash_command='echo All data have been transformed from ods to dw',
    )

    dag_start >> [load_crm_owner_to_ods, load_crm_opportunity_to_ods, load_crm_account_to_ods,
                  load_crm_activity_to_ods] >> ods_finished >> [dw_crm_owner, dw_crm_opportunity, dw_crm_activity,
                                                                dw_crm_account] >> dw_finished
