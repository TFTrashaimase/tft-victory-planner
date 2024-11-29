# Define the default arguments for the DAG
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'testing_dbt',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG with the specified schedule interval
dag = DAG('dbt_dag', default_args=default_args, schedule_interval=timedelta(days=1), catchup=False)

# Define the dbt run command as a BashOperator
run_dbt_model = BashOperator(
    task_id='run_dbt_model',
    bash_command='cd /opt/airflow/dbt && dbt build --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt',
    dag=dag
)
