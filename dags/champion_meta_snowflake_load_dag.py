# 수정된 부분은 다음 주석을 달아둠
# dbt_run_dag 개선

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta

# 환경 변수 가져오기
SNOWFLAKE_DATABASE = Variable.get("SNOWFLAKE_DATABASE", default_var=None)
SNOWFLAKE_SCHEMA = Variable.get("SNOWFLAKE_SCHEMA", default_var=None)
SNOWFLAKE_STAGE = Variable.get("SNOWFLAKE_STAGE_CHAMPION", default_var=None)
SNOWFLAKE_CHAMPION_INFO_TABLE = Variable.get("SNOWFLAKE_CHAMPION_INFO_TABLE", default_var=None)
BUCKET_NAME = Variable.get("BUCKET_NAME", default_var=None)
AWS_ACCESS_KEY = Variable.get("AWS_ACCESS_KEY", default_var=None)
AWS_SECRET_KEY = Variable.get("AWS_SECRET_KEY", default_var=None)

# dbt_run_dag 개선
# 함수 추가
def set_completed_flag_at_champ_meta(**kwargs):
    exe_datetime = kwargs['execution_date']
    exe_string = exe_datetime.strftime('%Y-%m-%d')

    Variable.set("trigger_from_champion_meta", exe_string)

# DAG 기본 설정
default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="champion_meta_snowflake_load_dag",
    default_args=default_args,
    schedule_interval=None,  # Trigger되어 실행됨
    catchup=False,
    description="DAG to load champion's metadata Parquet files from S3 to Snowflaktere, process, and clean stage",
) as snowflake_load_dag:

    # 1. Snowflake 스테이지에 데이터 적재
    load_data_to_stage = SnowflakeOperator(
        task_id="load_data_to_stage",
        snowflake_conn_id="snowflake_conn",
        sql=f"""CREATE OR REPLACE STAGE {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_STAGE}
            URL='s3://{BUCKET_NAME}/metadata_champion/{{{{ ds }}}}/'
            CREDENTIALS = (AWS_KEY_ID = '{{{{ var.value.AWS_ACCESS_KEY }}}}' AWS_SECRET_KEY = '{{{{ var.value.AWS_SECRET_KEY }}}}')
            FILE_FORMAT=(TYPE='PARQUET')""",
        autocommit=True,
    )

    # 2. 스테이지에 적재가 잘 되었는지 확인
    is_stage_data_ready = SnowflakeOperator(
        task_id="is_stage_data_ready",
        snowflake_conn_id="snowflake_conn",
        sql=f"LIST @{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_STAGE};",
        autocommit=True,
    )

    # 3. SNOWFLAKE_CHAMPION_INFO_TABLE 생성
    create_bronze_champion_info = SnowflakeOperator(
        task_id="create_bronze_champion_info",
        snowflake_conn_id="snowflake_conn",
        sql=f"""
            CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_CHAMPION_INFO_TABLE} (
                source STRING,
                ingestion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                data VARIANT
            );
        """,
        autocommit=True,
    )

    # 4. 스테이지에서 RAW_CHAMPION_META 테이블로 데이터 복사
    copy_into_bronze_champion_info = SnowflakeOperator(
        task_id="copy_into_bronze_champion_info",
        snowflake_conn_id="snowflake_conn",
        sql=f"""
            COPY INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_CHAMPION_INFO_TABLE}
            FROM (
                SELECT
                    's3://tft-team2-rawdata/metadata_champion/{{{{ ds }}}}/' AS source,
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
    # 5. 현재 DAG가 완료됐다는 걸 airflow 환경변수에 올림 ("CMSLD_COMPLETED": "true")
    set_completion_flag = PythonOperator(
        task_id="set_completion_flag_from_champ_meta",
        python_callable=set_completed_flag_at_champ_meta,
        provide_context=True,
    )

    # 6. DBT 트리거
    trigger_dbt_from_champ_meta = TriggerDagRunOperator(
        task_id='trigger_from_champ_meta',
        trigger_dag_id='dbt_run_dag', 
        conf={'trigger_source': 'trigger_from_champ_meta'}
    )

    # 작업 순서 정의
    (
        load_data_to_stage
        >> is_stage_data_ready
        >> create_bronze_champion_info
        >> copy_into_bronze_champion_info
        >> set_completion_flag  # dbt_run_dag 개선
        >> trigger_dbt_from_champ_meta
    )
