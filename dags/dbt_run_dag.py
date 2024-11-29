# Define the default arguments for the DAG
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

trigger_status = {"trigger_a": False, "trigger_b": False}

default_args = {
    'owner': 'dbt_runner',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def check_triggers(**kwargs):
    """
    트리거 상태 확인. 두 개의 트리거가 모두 True일 경우 True를 반환.
    """
    global trigger_status
    task = kwargs['task_instance']
    
    # XCom에서 이전 상태 로드
    prev_status = task.xcom_pull(task_ids='save_trigger_state', key='status')
    if prev_status:
        trigger_status.update(prev_status)
    
    # 두 개의 트리거가 모두 True인지 확인
    if trigger_status["trigger_a"] and trigger_status["trigger_b"]:
        print("All triggers received. Proceeding to dbt run.")
        return True
    else:
        print(f"Current trigger state: {trigger_status}")
        return False

def save_trigger_state(trigger_name, **kwargs):
    """
    트리거 상태를 저장. 실행될 때마다 트리거 상태를 업데이트.
    """
    global trigger_status
    trigger_status[trigger_name] = True
    
    # 상태 저장 (XCom에 저장)
    kwargs['task_instance'].xcom_push(key='status', value=trigger_status)


# Create the DAG with the specified schedule interval
with DAG('dbt_run_dag', default_args=default_args, schedule="@once", catchup=False) as dag:

    save_trigger_from_match_info = PythonOperator(
            task_id='save_trigger_from_match_info',
            python_callable=save_trigger_state,
            op_kwargs={'trigger_name': 'trigger_from_match_infos'},
            provide_context=True
        )

    save_trigger_from_champ_meta = PythonOperator(
            task_id='save_trigger_from_champ_meta',
            python_callable=save_trigger_state,
            op_kwargs={'trigger_name': 'trigger_from_champ_meta'},
            provide_context=True
        )

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

    [save_trigger_from_match_info, save_trigger_from_champ_meta] >> check_all_triggers >> run_dbt_model
