from airflow import DAG
from airflow.models import DagRun
from airflow.utils.db import provide_session
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'dbt_runner',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 1),
    'retry_delay': timedelta(minutes=1)
}

# 최근 실행 여부 체크 함수
@provide_session
def check_last_run(**kwargs, session=None):
    """ 최근 실행된 DAG이 1시간 내에 실행되었는지 확인 후, 트리거 여부 결정 """
    target_dag_id = "dbt_run_dag" 

    last_run = session.query(DagRun).filter(
        DagRun.dag_id == target_dag_id,
        DagRun.state == "success"  # 성공한 DAG만 체크
    ).order_by(DagRun.execution_date.desc()).first()

    if last_run:
        last_run_time = last_run.execution_date
        if (datetime.utcnow() - last_run_time).total_seconds() < 3600:
            print("Skipping DAG run: Last run was within the past hour.")
            return False  # 1시간 내 실행됨 → 실행하지 않음

    print("Running DAG: No recent run within the past hour.")
    return True  # 실행 가능

with DAG('dbt_run_dag', default_args=default_args, schedule="@once", catchup=False) as dag:

    check_to_run = PythonOperator(
            task_id='check_to_run',
            python_callable=check_last_run,
            provide_context=True,
        )

    run_dbt_model = BashOperator(
        task_id='run_dbt_model',
        bash_command='cd /opt/airflow/dbt && dbt build --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt',
        dag=dag
    )

    check_to_run >> run_dbt_model
