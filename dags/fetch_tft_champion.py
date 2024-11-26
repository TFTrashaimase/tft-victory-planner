import json
import requests
import logging
import base64
import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from io import BytesIO
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

logger = logging.getLogger(__name__)


# 모든 값을 동일한 길이로 맞추는 함수
# pyarrow를 이용해서 데이터를 테이블로 변환할 땐 동일한 길이로 맞춰야 하므로.
def normalize_json(data):
    # 모든 값을 리스트로 변환
    for key in data:
        if not isinstance(data[key], list):
            data[key] = [data[key]]

    # 최대 길이 계산
    max_length = max(len(value) for value in data.values())

    # 모든 값을 동일한 길이로 맞춤
    for key in data:
        data[key] = ensure_uniform_length(data[key], max_length)

    return data

# 리스트의 길이를 맞추는데 길이가 모자라면 None으로 패딩
def ensure_uniform_length(data_list, length):
    while len(data_list) < length:
        data_list.append(None)
    return data_list


def fetch_data(**kwargs):
    url = "https://ddragon.leagueoflegends.com/cdn/14.23.1/data/en_US/tft-champion.json"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        logger.info("success: fetch data 완료 (json)")

        # ti: task instance
        kwargs['ti'].xcom_push(key='raw_data', value=data)
    except requests.exceptions.RequestException as e:
        logger.error(f"error: fetch data 실패: {e}")
        raise

# json -> parquet
def transform_to_parquet(**kwargs):
    raw_data = kwargs['ti'].xcom_pull(key='raw_data', task_ids='fetch_data')
    if not raw_data:
        raise ValueError("no data: 'fetch_data'에서 넘어온 데이터가 없음")
    
    normalized_data = normalize_json(raw_data)

    try:
        table = pa.Table.from_pydict(normalized_data)

        parquet_buffer = BytesIO()
        pq.write_table(table, parquet_buffer)
        parquet_buffer.seek(0)
        logger.info("success: 메모리 내에서 parquet으로 변환 완료")

        parquet_base64 = base64.b64encode(parquet_buffer.read()).decode('utf-8')

        kwargs['ti'].xcom_push(key='parquet_data', value=parquet_base64)

    except Exception as e:
        logger.error(f"error: json -> parquet 도중 에러 발생: {e}")
        raise


def save_to_s3(**kwargs):
    parquet_base64 = kwargs['ti'].xcom_pull(key='parquet_data', task_ids='transform_to_parquet')
    if not parquet_base64:
        raise ValueError("no data: 'transform_to_parquet'에서 넘어온 데이터가 없음")
    
    parquet_data = base64.b64decode(parquet_base64)
    parquet_buffer = BytesIO(parquet_data)

    exe_datetime = kwargs['execution_date']
    exe_string = exe_datetime.strftime('%Y-%m-%d')
    file_name = f"{exe_string}/champion_data_{exe_string}.parquet"

    aws_access_key = Variable.get("AWS_ACCESS_KEY")
    aws_secret_key = Variable.get("AWS_SECRET_KEY")
    s3_bucket_name = Variable.get("BUCKET_NAME")

    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )
        s3_client.put_object(
            Bucket=s3_bucket_name,
            Key=file_name,
            Body=parquet_data,
            ContentType='application/octet-stream'
        )
        logger.info(f"success: S3에 데이터 업로드 완료 - //{s3_bucket_name}/{file_name}")
    except NoCredentialsError:
        logger.error("error: AWS 자격 증명 정보 없음")
        raise
    except PartialCredentialsError:
        logger.error("error: AWS 자격 증명 불완전함")
        raise
    except Exception as e:
        logger.error(f"error: S3에 데이터 업로드 실패: {e}")
        raise

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'tft_champion_data_ingestion',
    default_args=default_args,
    description='TFT 챔피언 데이터 수집(json) -> parquet로 변경 -> S3 적재',
    schedule_interval=None,
    catchup=False,
    tags=['riot', 'tft', 'champion']
) as dag:

    fetch_data_task = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data,
        provide_context=True,
    )

    transform_to_parquet_task = PythonOperator(
        task_id='transform_to_parquet',
        python_callable=transform_to_parquet,
        provide_context=True,
    )

    save_to_s3_task = PythonOperator(
        task_id='save_to_s3',
        python_callable=save_to_s3,
        provide_context=True,
    )

    fetch_data_task >> transform_to_parquet_task >> save_to_s3_task
