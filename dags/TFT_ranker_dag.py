import json
import os
import logging
import requests
import boto3
import time

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

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
MATCHES_COUNT = 20  # 한 번에 가져올 매치 수

if not API_KEY or not BASE_URL:
    raise ValueError("환경 변수 RIOT_API_KEY와 RIOT_API_BASE_URL의 확인이 필요합니다.")

# s3 버킷 설정
s3 = boto3.client('s3', region_name='ap-northeast-2')
BUCKET_NAME = 'our_bucket_name'  # 어떻게 처리하실 건지 결정 필요합니다. => 환경변수, Variables

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

# 티어별 puuid를 합친 리스트를 제공
def process_puuid_data(**kwargs):
    ti = kwargs['ti']
    challenger_data = ti.xcom_pull(task_ids='get_challenger_task')
    grandmaster_data = ti.xcom_pull(task_ids='get_grandmaster_task')
    master_data = ti.xcom_pull(task_ids='get_master_task')
    tier_data = ti.xcom_pull(task_ids='get_tier_task')

    raw_puuid_data = list(challenger_data + grandmaster_data + master_data + tier_data)

    return raw_puuid_data

# API를 호출하여 Matching ID 목록을 가져오는 함수
def get_matching_ids(puuid, **kwargs):
    url = f"{BASE_URL}/tft/match/v1/matches/by-puuid/{puuid}/ids?start=0&count={MATCHES_COUNT}"
    request_header = {
        "X-Riot-Token": API_KEY,
        "Content-Type": "application/json;charset=utf-8"
    }

    try:
        response = requests.get(url, headers=request_header)
        response.raise_for_status()
        data = response.json()

        if len(data) > 0:
            logging.info(f"Fetched {len(data)} matching IDs for puuid: {puuid}")
            return data
        else:
            logging.info(f"No matching IDs found for puuid: {puuid}")
            return []

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching matching IDs for puuid: {puuid}: {e}")
        return []

# matching id를 하나의 리스트로 정리
def process_matching_ids(**kwargs):
    # TFT_ranker_dag.py의 process_data_task에서 puuid_list를 가져옵니다 (XCom을 사용)
    puuid_list = kwargs['ti'].xcom_pull(task_ids='process_data_task')  # 'process_data_task' 사용
    full_matching_ids = []

    if puuid_list:
        for puuid in puuid_list:
            # puuid에 대해 matching ID를 가져옵니다
            time.sleep(1)
            matching_ids = get_matching_ids(puuid)
            full_matching_ids += matching_ids
            logging.info(f"Matching IDs for puuid {puuid}: {matching_ids}")
    else:
        logging.error(f"{puuid}: No match data for this puuid found from process_data_task in TFT_ranker_dag.py")
    
    return full_matching_ids

# s3에 적재
def load_json_to_s3(**kwargs):
    exe_datetime = kwargs['execution_date']  # execution_date 기준으로 폴더 명을 나눔
    exe_string = exe_datetime.strftime('%Y-%m-%d')
    match_data = kwargs['ti'].xcom_pull(task_ids='get_matching_ids_task')  # 매치데이터를 받아옴

    if not match_data:
        logging.info("No match data to process")
        return exe_string

    for match in match_data:
        time.sleep(1)
        match_api_url = f"https://asia.api.riotgames.com/tft/match/v1/matches/{match}"
        response = requests.get(match_api_url, headers={"X-Riot-Token": API_KEY})
        response.raise_for_status()
        data = response.json()

        file_name = exe_string + '/' + match + '_' + exe_string + '.json'
        try:
            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=file_name,
                Body=json.dumps(data),
                ContentType='application/json'
            )
        except boto3.exceptions.Boto3Error as e:
            logging.error(f"Failed to upload {file_name} to S3: {e}")
    
    return exe_string

# PythonOperator 정의
# Matching ID 목록을 가져오는 작업입니다.
matching_id_task = PythonOperator(
    task_id='get_matching_ids_task',
    python_callable=process_matching_ids,
    provide_context=True,
    dag=dag
)


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

# 상위 유저정보들을 하나의 리스트로 정리
process_puuid = PythonOperator(
    task_id = 'process_puuid_raw_data',
    python_callable=process_puuid_data,
    provide_context=True,
    dag=dag
)

# matchId를 받아오는 태스트
matching_id_task = PythonOperator(
    task_id='get_matching_ids_task',
    python_callable=process_matching_ids,
    provide_context=True,
    dag=dag
)

# s3에 업로드하는 task
s3_json_load_task = PythonOperator(
    task_id='load_json_to_s3',
    python_callable=load_json_to_s3,
    provide_context=True,
    dag=dag
)

# 트리거할 DAG ID 리스트
dag_ids_to_trigger = ['dag_ids_to_transform_and_load_data_to_snowflake']

with TaskGroup(group_id='trigger_snowflake_load_dags') as trigger_group:
    for dag_id in dag_ids_to_trigger:
        conf = {
                "s3_bucket_folder": "{{ task_instance.xcom_pull(task_ids='load_json_to_s3') }}",
                "triggered_by": "trigger_dynamic_dags", 
                "triggered_dag": dag_id,
                "execution_date": "{{ execution_date.isoformat() }}",
            }
        TriggerDagRunOperator(task_id=f'trigger_{dag_id}', trigger_dag_id=dag_id, conf=conf)


# 태스크 의존성 설정
[challenger_task, grandmaster_task, master_task, tier_task] >> process_puuid >> matching_id_task >> s3_json_load_task >> trigger_group