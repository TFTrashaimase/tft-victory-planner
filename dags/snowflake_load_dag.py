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

API_KEY = Variable.get("API_KEY", default_var=None)
BUCKET_NAME = Variable.get("BUCKET_NAME", default_var=None)
AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY", default_var=None)
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_KEY", default_var=None)


# DAG 기본 설정
default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}


with DAG(
    'snowflake_load_dag',
    default_args=default_args,
    schedule_interval=None,  # Trigger되어 실행됨
    catchup=False,
) as snowflake_load_dag:

    # S3 -> Snowflake
    load_to_snowflake = S3ToSnowflakeOperator(
        task_id='load_s3_to_snowflake',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        s3_bucket=BUCKET_NAME,
        s3_keys=[f"{{ ds }}/*.parquet"],
        snowflake_schema=SNOWFLAKE_SCHEMA,
        snowflake_database=SNOWFLAKE_DATABASE,
        file_format='parquet',
    )

    # Snowflake s3_stage에 적재
    stage_data_to_snowflake = SnowflakeOperator(
        task_id='stage_data_to_snowflake',
        sql="""
            LIST @BRONZE_STAGE;
        """,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        autocommit=True,
    )

    # s3_stage에서 공용 테이블 COPY INTO
    copy_into_snowflake = SnowflakeOperator(
        task_id='copy_into_snowflake',
        sql=f"""
            COPY INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.TFT_BRONZE_TABLE (source, data)
            FROM @BRONZE_STAGE/
            FILE_FORMAT = (TYPE = 'PARQUET');
        """,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        autocommit=True,
    )


    # 작업 순서 설정
    load_to_snowflake >> stage_data_to_snowflake >> copy_into_snowflake

