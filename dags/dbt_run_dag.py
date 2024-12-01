# Define the default arguments for the DAG
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'dbt_runner',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 1),
    'retry_delay': timedelta(minutes=1)
}

def check_triggers(**kwargs):
    """
    트리거 상태 확인. 두 개의 트리거가 모두 True일 경우 True를 반환.
    """
    exe_datetime = kwargs['execution_date']
    exe_string = exe_datetime.strftime('%Y-%m-%d')

    trigger_from_match_info = Variable.get("trigger_from_match_info", "true")
    trigger_from_champ_meta = Variable.get("trigger_from_champion_meta", "true")

    # dbt_run_dag 개선
    # 두 트리거가 모두 True인지 확인
    if trigger_from_champ_meta == exe_string and trigger_from_match_info == exe_string:
        print("All triggers received. Proceeding to dbt run.")
        return True
    else:
        print(f"Current trigger state:")
        print("match_info: ", f"{trigger_from_match_info}")
        print("champ_meta: ", f"{trigger_from_champ_meta}")
        raise Exception("Not both dags are completed")

# Create the DAG with the specified schedule interval
with DAG('dbt_run_dag', default_args=default_args, schedule="@once", catchup=False) as dag:

    check_all_triggers = PythonOperator(
            task_id='check_all_triggers',
            python_callable=check_triggers,
            provide_context=True,
        )

    # Define the dbt run command as a BashOperator
    run_dbt_model = BashOperator(
        task_id='run_dbt_model',
        bash_command='cd /opt/airflow/dbt && dbt build --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt',
        dag=dag
    )

    check_all_triggers >> run_dbt_model