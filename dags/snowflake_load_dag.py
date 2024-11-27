from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.models import Variable
from datetime import datetime, timedelta

# 환경 변수 가져오기
SNOWFLAKE_CONN_ID = Variable.get("SNOWFLAKE_CONN_ID", default_var="snowflake_default")
SNOWFLAKE_WAREHOUSE = Variable.get('SNOWFLAKE_WAREHOUSE', default_var=None)
SNOWFLAKE_DATABASE = Variable.get('SNOWFLAKE_DATABASE', default_var=None)
SNOWFLAKE_SCHEMA = Variable.get('SNOWFLAKE_SCHEMA', default_var=None)
SNOWFLAKE_STAGE = Variable.get('SNOWFLAKE_STAGE', default_var=None)
BUCKET_NAME = Variable.get("BUCKET_NAME", default_var=None)

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
    description="DAG to load Parquet files from S3 to Snowflake, process, and clean stage",
) as snowflake_load_dag:

    # 1. S3 -> Snowflake로 데이터 적재
    load_s3_to_snowflake = S3ToSnowflakeOperator(
        task_id='load_s3_to_snowflake',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        s3_bucket=BUCKET_NAME,
        s3_keys=["{{ ds }}/*.parquet"],  # 매일 적재를 통해 생성되는 '날짜 형식의 S3 폴더'로부터 parquet 파일만 가져오기
        snowflake_schema=SNOWFLAKE_SCHEMA,
        snowflake_database=SNOWFLAKE_DATABASE,
        file_format='parquet', 
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
    load_s3_to_snowflake >> stage_data_to_snowflake >> copy_into_snowflake >> clean_stage_data
