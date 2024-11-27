from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta

# 환경 변수 가져오기
# SNOWFLAKE_CONN_ID = Variable.get("SNOWFLAKE_CONN_ID", default_var="snowflake_default")
SNOWFLAKE_WAREHOUSE = Variable.get('SNOWFLAKE_WAREHOUSE', default_var=None)
SNOWFLAKE_DATABASE = Variable.get('SNOWFLAKE_DATABASE', default_var=None)
SNOWFLAKE_SCHEMA = Variable.get('SNOWFLAKE_SCHEMA', default_var=None)
SNOWFLAKE_STAGE = Variable.get('SNOWFLAKE_STAGE', default_var=None)
BUCKET_NAME = Variable.get("BUCKET_NAME", default_var=None)
AWS_ACCESS_KEY = Variable.get("AWS_ACCESS_KEY", default_var=None)
AWS_SECRET_KEY = Variable.get("AWS_SECRET_KEY", default_var=None)

# SNOWFLAKE_CONN_ID = BaseHook.get_connection("CONN_SNOWFLAKE")
SNOWFLAKE_CONN_ID = "conn_snowflake"

# DAG 기본 설정
default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='snowflake_load_dag',
    default_args=default_args,
    schedule_interval=None,  # Trigger되어 실행됨
    catchup=False,
    description="DAG to load Parquet files from S3 to Snowflaktere, process, and clean stage",
) as snowflake_load_dag:

    # 1. Snowflake 스테이지에 데이터 적재
    load_data_to_stage = SnowflakeOperator(
        task_id='load_data_to_stage',
        sql=f"""
            COPY INTO @{SNOWFLAKE_STAGE}/
            FROM 's3://{BUCKET_NAME}/{{{{ ds }}}}/'
            CREDENTIALS = (AWS_KEY_ID = '{{{{ var.value.AWS_ACCESS_KEY }}}}' AWS_SECRET_KEY = '{{{{ var.value.AWS_SECRET_KEY }}}}')
            FILE_FORMAT = (TYPE = 'PARQUET');
        """,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        autocommit=True,
    )

    # 2. 스테이지 파일 확인
    stage_data_to_snowflake = SnowflakeOperator(
        task_id='stage_data_to_snowflake',
        sql=f"LIST @{SNOWFLAKE_STAGE};",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        autocommit=True,
    )

    # 3. 스테이지에서 Snowflake 테이블로 데이터 복사
    copy_into_snowflake = SnowflakeOperator(
        task_id='copy_into_snowflake',
        sql=f"""
            COPY INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.TFT_BRONZE_TABLE
            FROM @{SNOWFLAKE_STAGE}/
            FILE_FORMAT = (TYPE = 'PARQUET');
        """,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        autocommit=True,
    )

    # 4. 스테이지 내부 파일 정리
    clean_stage_data = SnowflakeOperator(
        task_id="clean_stage_data",
        sql=f"REMOVE @{SNOWFLAKE_STAGE};",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        autocommit=True,
    )

    # 작업 순서 정의
    load_data_to_stage >> stage_data_to_snowflake >> copy_into_snowflake >> clean_stage_data

