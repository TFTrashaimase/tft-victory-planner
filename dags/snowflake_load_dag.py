from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.models import Variable
from datetime import datetime, timedelta

from airflow import settings
from airflow.models import Connection
import json

# 환경 변수 가져오기
#SNOWFLAKE_CONN_ID = Variable.get("SNOWFLAKE_CONN_ID", default_var="snowflake_default") 
SNOWFLAKE_ACCOUNT_ID = Variable.get("SNOWFLAKE_ACCOUNT_ID", default_var=None)
SNOWFLAKE_WAREHOUSE = Variable.get("SNOWFLAKE_WAREHOUSE", default_var=None)
SNOWFLAKE_ROLE = Variable.get("SNOWFLAKE_ROLE", default_var=None)
SNOWFLAKE_ACCOUNT_NAME = Variable.get("SNOWFLAKE_ACCOUNT_NAME", default_var=None)
SNOWFLAKE_DATABASE = Variable.get("SNOWFLAKE_DATABASE", default_var=None)
SNOWFLAKE_ACCOUNT_PW = Variable.get("SNOWFLAKE_ACCOUNT_PW", default_var=None)
SNOWFLAKE_SCHEMA = Variable.get("SNOWFLAKE_SCHEMA", default_var=None)
SNOWFLAKE_CONN_ID = Variable.get("SNOWFLAKE_CONN_ID", default_var="snowflake_default")
SNOWFLAKE_STAGE = Variable.get("SNOWFLAKE_STAGE", default_var=None)
BUCKET_NAME = Variable.get("BUCKET_NAME", default_var=None)
AWS_ACCESS_KEY = Variable.get("AWS_ACCESS_KEY", default_var=None)
AWS_SECRET_KEY = Variable.get("AWS_SECRET_KEY", default_var=None)

# SNOWFLAKE_CONN_ID = "snowflake_default"

# 임시 Snowflake_conn_id
# SNOWFLAKE_CONN_ID="conn_snowflake_2"

def create_snowflake_connection():
    print(f"SNOWFLAKE_ACCOUNT_ID: {SNOWFLAKE_ACCOUNT_ID}")
    print(f"SNOWFLAKE_WAREHOUSE: {SNOWFLAKE_WAREHOUSE}")
    print(f"SNOWFLAKE_ROLE: {SNOWFLAKE_ROLE}")
    print(f"SNOWFLAKE_DATABASE: {SNOWFLAKE_DATABASE}")
    print(f"SNOWFLAKE_CONN_ID: {SNOWFLAKE_CONN_ID}")
    print(f"SNOWFLAKE_ACCOUNT_NAME: {SNOWFLAKE_ACCOUNT_NAME}")
    print(f"SNOWFLAKE_ACCOUNT_PW: {SNOWFLAKE_ACCOUNT_PW}")
    print(f"SNOWFLAKE_SCHEMA: {SNOWFLAKE_SCHEMA}")

    extra = {
        "account": SNOWFLAKE_ACCOUNT_ID,
        "warehouse": SNOWFLAKE_WAREHOUSE,
        "role": SNOWFLAKE_ROLE,
        "database": SNOWFLAKE_DATABASE
    }

    session = settings.Session()
    existing_connection = session.query(Connection).filter(Connection.conn_id == SNOWFLAKE_CONN_ID).first()

    if not existing_connection:
        connection = Connection(
            conn_id=SNOWFLAKE_CONN_ID,
            conn_type="snowflake",
            login=SNOWFLAKE_ACCOUNT_NAME,
            password=SNOWFLAKE_ACCOUNT_PW,
            schema=SNOWFLAKE_SCHEMA,
            extra=json.dumps(extra)
        )
        session.add(connection)
        session.commit()
        print("Connections 설정 완료")
    else:
        print("이미 동일한 conn_id의 Connections가 존재함")

    session.close()

create_snowflake_connection()

# DAG 기본 설정
default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="snowflake_load_dag",
    default_args=default_args,
    schedule_interval=None,  # Trigger되어 실행됨
    catchup=False,
    description="DAG to load Parquet files from S3 to Snowflaktere, process, and clean stage",
) as snowflake_load_dag:

    # 1. Snowflake 스테이지에 데이터 적재
    load_data_to_stage = SnowflakeOperator(
        task_id="load_data_to_stage",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""CREATE OR REPLACE STAGE {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_STAGE}
            URL='s3://{BUCKET_NAME}/{{{{ ds }}}}/match_infos/'
            CREDENTIALS = (AWS_KEY_ID = '{{{{ var.value.AWS_ACCESS_KEY }}}}' AWS_SECRET_KEY = '{{{{ var.value.AWS_SECRET_KEY }}}}')
            FILE_FORMAT=(TYPE='PARQUET')""",
        autocommit=True,
    )

    # # 2. 스테이지에 적재가 잘 되었는지 확인
    is_stage_data_ready = SnowflakeOperator(
        task_id="is_stage_data_ready",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"LIST @{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_STAGE};",
        autocommit=True,
    )

    # # 3 - 1. 스테이지에서 BRONZE_TFT_CHAMPION_INFO 테이블로 데이터 복사
    copy_into_bronze_champion_info = SnowflakeOperator(
        task_id="copy_into_bronze_champion_info",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
            COPY INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.BRONZE_TFT_CHAMPION_INFO
            FROM (
                SELECT
                's3://{BUCKET_NAME}/{{{{ ds }}}}' AS source,
                CURRENT_TIMESTAMP() AS ingestion_date,
                $1 data
                FROM @{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_STAGE}
            )
            FILE_FORMAT = (TYPE = 'PARQUET')
            PATTERN = '^(?!.*match_infos/).*\.parquet';
        """,
        autocommit=True,
    )
    
        # # 3 - 2. 스테이지에서 BRONZE_TFT_MATCH_INFO 테이블로 데이터 복사
    copy_into_bronze_match_info = SnowflakeOperator(
        task_id="copy_into_bronze_match_info",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
            COPY INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.BRONZE_TFT_MATCH_INFO
            FROM (
                SELECT
                's3://{BUCKET_NAME}/{{{{ ds }}}}' AS source,
                CURRENT_TIMESTAMP() AS ingestion_date,
                $1 data
                FROM @{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_STAGE}
            )
            FILE_FORMAT = (TYPE = 'PARQUET')
            PATTERN = '.*match_infos/.*\.parquet';
        """,
        autocommit=True,
    )

    # # 4. 스테이지 내부 파일 정리
    clean_stage_data = SnowflakeOperator(
        task_id="clean_stage_data",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"REMOVE @{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_STAGE};",
        autocommit=True,
    )

    # 작업 순서 정의
    (
        load_data_to_stage
        >> is_stage_data_ready
        >> [copy_into_bronze_champion_info, copy_into_bronze_match_info]
        >> clean_stage_data
    )
