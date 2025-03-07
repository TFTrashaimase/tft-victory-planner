from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta

# 환경 변수 가져오기  
SNOWFLAKE_CONN_ID = Variable.get("SNOWFLAKE_CONN_ID", default_var="snowflake_default")
SNOWFLAKE_DATABASE = Variable.get("SNOWFLAKE_DATABASE", default_var=None)
SNOWFLAKE_SCHEMA = Variable.get("SNOWFLAKE_SCHEMA", default_var=None) 
SNOWFLAKE_STAGE = Variable.get("SNOWFLAKE_STAGE_MATCH", default_var=None)
BUCKET_NAME = Variable.get("BUCKET_NAME", default_var=None)
AWS_ACCESS_KEY = Variable.get("AWS_ACCESS_KEY", default_var=None)
AWS_SECRET_KEY = Variable.get("AWS_SECRET_KEY", default_var=None)
SNOWFLAKE_MATCH_INFO_TABLE = Variable.get("SNOWFLAKE_MATCH_INFO_TABLE", default_var=None)

# DAG 기본 설정
default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

def set_completed_flag_at_match_info(**kwargs):
    exe_datetime = kwargs['execution_date']
    exe_string = exe_datetime.strftime('%Y-%m-%d')
    
    Variable.set("trigger_from_match_info", exe_string)

with DAG(
    dag_id="match_info_snowflake_load_dag",
    default_args=default_args,
    schedule_interval=None,  # Trigger되어 실행됨
    catchup=False,
    description="DAG to load Parquet files from S3 to Snowflaktere, process, and clean stage",
) as snowflake_load_dag:


    # 1. Snowflake 스테이지에 데이터 적재
    load_data_to_stage = SnowflakeOperator(
        task_id="load_data_to_stage",
        snowflake_conn_id="snowflake_conn",
        sql=f"""
            CREATE OR REPLACE STAGE {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_STAGE}
                URL='s3://{BUCKET_NAME}/match_infos/{{{{ ds }}}}/' 
                CREDENTIALS = (AWS_KEY_ID = '{{{{ var.value.AWS_ACCESS_KEY }}}}'AWS_SECRET_KEY = '{{{{ var.value.AWS_SECRET_KEY }}}}')
            FILE_FORMAT=(TYPE='PARQUET')
            """,
        autocommit=True,
    )

    # # 2. 스테이지에 적재가 잘 되었는지 확인
    is_stage_data_ready = SnowflakeOperator(
        task_id="is_stage_data_ready",
        snowflake_conn_id="snowflake_conn",
        sql=f"LIST @{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_STAGE};",
        autocommit=True,
    )

    # # 3. SNOWFLAKE_MATCH_INFO_TABLE 생성
    create_bronze_match_info = SnowflakeOperator(
        task_id="create_bronze_match_info",
        snowflake_conn_id="snowflake_conn",
        sql=f"""
            CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_MATCH_INFO_TABLE} (
                source STRING,
                ingestion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                data VARIANT
            );
        """,
        autocommit=True,
    )

    # # 4. 스테이지에서 SNOWFLAKE_MATCH_INFO_TABLE로 데이터 복사
    copy_into_bronze_match_info = SnowflakeOperator(
        task_id="copy_into_bronze_match_info",
        snowflake_conn_id="snowflake_conn",
        sql=f"""
            COPY INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_MATCH_INFO_TABLE}
            FROM (
                SELECT
                    's3://{BUCKET_NAME}/match_infos/{{{{ ds }}}}/' AS source,
                    CURRENT_TIMESTAMP() AS ingestion_date,
                    $1 data
                FROM @{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_STAGE}
            )
            FILE_FORMAT = (TYPE = 'PARQUET')
            PATTERN = '.*\.parquet';
        """,
        autocommit=True,
    )

    # dbt_run_dag 개선
    set_completion_flag = PythonOperator(
        task_id="set_completion_flag_from_match_info",
        python_callable=set_completed_flag_at_match_info,
        provide_context=True,
    )

    # 5. DBT 트리거
    trigger_dbt_from_match_info = TriggerDagRunOperator(
        task_id='trigger_from_match_infos',
        trigger_dag_id='dbt_run_dag',  
        conf={'trigger_source': 'trigger_from_match_infos'}
    )

    # 작업 순서 정의
    (
        load_data_to_stage
        >> is_stage_data_ready
        >> create_bronze_match_info
        >> copy_into_bronze_match_info
        >> set_completion_flag
        >> trigger_dbt_from_match_info
    )
