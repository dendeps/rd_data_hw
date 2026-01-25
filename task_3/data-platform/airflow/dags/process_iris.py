from datetime import datetime, timedelta
import os
import random
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from python_scripts.train_model import process_iris_data
from zoneinfo import ZoneInfo
from airflow.utils import timezone


# Import our custom operator
from dbt_operator import DbtOperator

# Get environment variables
ANALYTICS_DB = os.getenv('ANALYTICS_DB', 'analytics')
PROJECT_DIR = os.getenv('AIRFLOW_HOME')+"/dags/dbt/homework"
PROFILE = 'homework'

DEFAULT_ARGS = {
    'owner': 'airflow',
    "email": ["your_email@gmail.com"], 
    'depends_on_past': False,
    "email_on_success": True,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Environment variables to pass to dbt
env_vars = {
    'ANALYTICS_DB': ANALYTICS_DB,
    'DBT_PROFILE': PROFILE
}

# Example of variables to pass to dbt
dbt_vars = {
    'is_test': False,
    'data_date': '{{ ds }}',  # Uses Airflow's ds (execution date) macro
}


with DAG(
    dag_id="process_iris",
    default_args=DEFAULT_ARGS,
    start_date=timezone.datetime(2025, 12, 1, 1, 0),  # uses Airflow default timezone
    schedule_interval="0 1 * * *",  # 01:00 за Києвом
    catchup=True,
    max_active_runs=4,
) as dag:

    # Step 1: Run dbt run to execute models
    t1_run_dbt = DbtOperator(
        task_id='dbt_run',
        dag=dag,
        command='run',
        profile=PROFILE,
        project_dir=PROJECT_DIR,
        # Example of selecting specific models
        # models=['example'],  # This selects all staging model
        env_vars=env_vars,
        vars=dbt_vars,
    )

    # generate the INSERT SQL at runtime and push via XCom
    t2_train_model = PythonOperator(
        task_id="train_model",
        python_callable=process_iris_data
    )

    

    t1_run_dbt >> t2_train_model 