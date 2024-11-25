from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable  # Airflow Variable 사용
from datetime import datetime
import requests
import json

import os  # 테스트용 로컬 저장을 위해 추가함

# 기존 변수 (S3 관련 코드는 주석 처리)
# BUCKET_NAME = Variable.get("BUCKET_NAME", default_var="tft-team2-rawdata")
# BUCKET_REGION = Variable.get("BUCKET_REGION", default_var="ap-northeast-2")
# RAW_DATA_PATH = "tft_champion_data/"  # S3 저장 경로
DATA_URL = "https://ddragon.leagueoflegends.com/cdn/14.23.1/data/en_US/tft-champion.json"

LOCAL_SAVE_DIR = os.path.join(os.getcwd(), "tmp")


def fetch_data(**kwargs):
    response = requests.get(DATA_URL)
    response.raise_for_status()
    data = response.json()
    return data


# S3 저장
# def save_to_s3(data, **kwargs):
#     s3 = boto3.client("s3", region_name=BUCKET_REGION)
#     file_name = f"{RAW_DATA_PATH}champion_data.json"
#     s3.put_object(Bucket=BUCKET_NAME, Key=file_name, Body=json.dumps(data))
#     print(f"S3 - 챔피언 데이터 저장 완료: {file_name}")

# 로컬 저장 (테스트용)
def save_to_local(data, **kwargs):
    ti = kwargs["ti"]
    data = ti.xcom_pull(task_ids="fetch_data")
    os.makedirs(LOCAL_SAVE_DIR, exist_ok=True)
    file_path = os.path.join(LOCAL_SAVE_DIR, "champion_data.json")
    with open(file_path, "w") as f:
        json.dump(data, f, indent=4)
    print(f"로컬 저장 완료: {file_path}")

with DAG(
    dag_id="fetch_tft_champion_data_local",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    fetch_champion_data = PythonOperator(
        task_id="fetch_data",
        python_callable=fetch_data,
        provide_context=True,
    )

    save_local = PythonOperator(
        task_id="save_to_local",
        python_callable=save_to_local,
        provide_context=True,
        op_kwargs={"data": "{{ ti.xcom_pull(task_ids='fetch_data') }}"},
    )

    fetch_champion_data >> save_local
