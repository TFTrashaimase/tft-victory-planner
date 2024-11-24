import json
import os
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
import requests

# API 설정

# docker환경 변수에서 API 키 읽을 때,사용
API_KEY = os.getenv("RIOT_API_KEY", "No API Key Provided")

# airflow Variables 사용할 때,
# API_KEY = Variable.get("RIOT_API_KEY") # 변수 설정 후 사용
BASE_URL = 'https://kr.api.riotgames.com'
QUEUE_TYPE = 'RANKED_TFT'  # 솔로 랭크 큐 타입
tiers = ["DIAMOND", "EMERALD", "PLATINUM", "GOLD", "SILVER", "BRONZE"]
divisions = ["I", "II", "III", "IV"]
page = 1

if not API_KEY or not BASE_URL:
    raise ValueError("환경 변수 RIOT_API_KEY와 RIOT_API_BASE_URL의 확인이 필요합니다.")

# DAG 기본 설정
default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'TFT_Riot_API_Dag',
    default_args=default_args,
    description='Get data from Riot API for TFT rankings',
    schedule_interval='0 0 * * *',  # 매일 자정에 실행
    catchup=False,  # 과거 실행 날짜에 대해 실행하지 않음
    tags=['riot', 'tft']
)

# 데이터 수집 함수
# 챌린저 랭킹 데이터 수집
def get_challenger(**kwargs):
    url = f"{BASE_URL}/tft/league/v1/challenger?queue={QUEUE_TYPE}"
    try:
        response = requests.get(url, headers={"X-Riot-Token": API_KEY})
        response.raise_for_status()
        data = response.json()
        if len(data) > 0:
            return data
        else:
            logging.info("No Challenger players found")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching Challenger data: {e}")
        return None

# 그랜드마스터 랭킹 데이터 수집
def get_grandmaster(**kwargs):
    url = f"{BASE_URL}/tft/league/v1/grandmaster?queue={QUEUE_TYPE}"
    try:
        response = requests.get(url, headers={"X-Riot-Token": API_KEY})
        response.raise_for_status()
        data = response.json()
        if len(data) > 0:
            return data
        else:
            logging.info("No Grandmaster players found")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching Grandmaster data: {e}")
        return None

# 마스터 랭킹 데이터 수집
def get_master(**kwargs):
    url = f"{BASE_URL}/tft/league/v1/master?queue={QUEUE_TYPE}"
    try:
        response = requests.get(url, headers={"X-Riot-Token": API_KEY})
        response.raise_for_status()
        data = response.json()
        if len(data) > 0:
            return data
        else:
            logging.info("No Master players found")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching Master data: {e}")
        return None

# 티어 랭킹 top100+데이터 수집
def get_tier(**kwargs):  
    for tier in tiers:
        for division in divisions:
            url = f"{BASE_URL}/tft/league/v1/entries/{tier}/{division}?queue={QUEUE_TYPE}&page={page}"
            try:
                response = requests.get(url, headers={"X-Riot-Token": API_KEY})
                response.raise_for_status()
                data = response.json()

                if len(data) >= 100:
                    logging.info(f"Fetched {len(data)} records for Tier={tier}, Division={division}")
                    return data

            except requests.exceptions.RequestException as e:
                logging.error(f"Error fetching data for Tier={tier}, Division={division}: {e}")
                continue
    return data

# PythonOperator 정의
# 챌린저 랭킹 데이터 수집
challenger_task = PythonOperator(
    task_id='get_challenger_task',
    python_callable=get_challenger,
    provide_context=True,
    dag=dag
)

# 그랜드마스터 랭킹 데이터 수집
grandmaster_task = PythonOperator(
    task_id='get_grandmaster_task',
    python_callable=get_grandmaster,
    provide_context=True,
    dag=dag
)

# 마스터 랭킹 데이터 수집
master_task = PythonOperator(
    task_id='get_master_task',
    python_callable=get_master,
    provide_context=True,
    dag=dag
)

# 티어 랭킹 top100+데이터 수집
tier_task = PythonOperator(
    task_id='get_tier_task',
    python_callable=get_tier,
    provide_context=True,
    dag=dag
)

# 다음 태스크에서 XCom 데이터를 활용시 사용 함수
def process_data(**kwargs):
    challenger_data = kwargs['ti'].xcom_pull(task_ids='get_challenger_task')
    grandmaster_data = kwargs['ti'].xcom_pull(task_ids='get_grandmaster_task')
    master_data = kwargs['ti'].xcom_pull(task_ids='get_master_task')
    tier_data = kwargs['ti'].xcom_pull(task_ids='get_tier_task')

    # 여기서 데이터를 처리하거나 다른 작업 수행 가능
    logging.info("Challenger Data: %s", challenger_data)
    logging.info("Grandmaster Data: %s", grandmaster_data)
    logging.info("Master Data: %s", master_data)
    logging.info("Tier Data: %s", tier_data)

# 데이터 처리 함수
process_task = PythonOperator(
    task_id='process_data_task',
    python_callable=process_data,
    provide_context=True,
    dag=dag
)

# 태스크 의존성 설정
[challenger_task, grandmaster_task, master_task, tier_task] >> process_task