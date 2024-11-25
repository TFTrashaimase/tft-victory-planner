import logging
import requests
import boto3
import time
import pyarrow as pa
import pyarrow.parquet as pq

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

# Variables 읽어오기 - .env 참고
API_KEY = Variable.get("API_KEY", default_var=None)
BASE_URL = Variable.get("BASE_URL", default_var=None)
QUEUE_TYPE = Variable.get("QUEUE_TYPE", default_var=None)
BUCKET_NAME = Variable.get("BUCKET_NAME", default_var=None)
BUCKET_REGION = Variable.get("BUCKET_REGION", default_var=None)

# s3 버킷 설정
s3 = boto3.client('s3', region_name='ap-northeast-2')  # 서울 리전 

tiers = ["DIAMOND", "EMERALD", "PLATINUM", "GOLD", "SILVER", "BRONZE"]
divisions = ["I", "II", "III", "IV"]
page = 1
MATCHES_COUNT = 20  # 한 번에 가져올 매치 수

if not API_KEY or not BASE_URL or not QUEUE_TYPE or not BUCKET_NAME or not BUCKET_REGION:
    raise ValueError("환경 변수 API_KEY, BASE_URL, QUEUE_TYPE, BUCKET_NAME의 확인이 필요합니다.")

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
                    logging.info(f"Fetched {len(data)} records for Tier={tier}, Divisiozn={division}")
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

    raw_puuid_data = list(challenger_data if challenger_data else []) + list(grandmaster_data if grandmaster_data else []) \
        + list(master_data if master_data else []) + list(tier_data if tier_data else [])

    exe_datetime = kwargs['execution_date']  # execution_date 기준으로 폴더 명을 나눔
    exe_string = exe_datetime.strftime('%Y-%m-%d')
    raw_puuid_data_dict = {exe_string + '_puuid_data': raw_puuid_data}

    table = pa.Table.from_pydict(raw_puuid_data_dict)

    buffer = pa.BufferOutputStream()
    pq.write_table(table, buffer)

    # 3. S3 업로드
    file_name = exe_string + '/' + 'puuid' + '_' + exe_string + '.parquet'
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=file_name,
        Body=buffer.getvalue(),
        ContentType='application/octet-stream'  # Parquet 파일에 적합한 MIME 타입
    )

    return raw_puuid_data

# API를 호출하여 Matching ID 목록을 가져오는 함수
def get_matching_ids(puuid, **kwargs):
    url = f"{BASE_URL}/tft/match/v1/matches/by-puuid/{puuid}/ids?start=0&count={MATCHES_COUNT}"
    request_header = {
        "X-Riot-Token": API_KEY,
        "Content-Type": "application/json;charset=utf-8"
    }

    if MATCHES_COUNT <= 200:
        try:
            response = requests.get(url, headers=request_header)
            response.raise_for_status()
            data = response.json()

            if len(data) > 0:
                logging.info(f"Fetched {len(data)} matching IDs for puuid: {puuid}")
                return list(data)
            else:
                logging.info(f"No matching IDs found for puuid: {puuid}")
                return []

        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching matching IDs for puuid: {puuid}: {e}")
            return []
    else:
        try:
            how_many_api_call, remain_api_call = MATCHES_COUNT // 200, MATCHES_COUNT % 200
            full_return_data = []
            for cnt in range(how_many_api_call):
                time.sleep(1)
                url = f"{BASE_URL}/tft/match/v1/matches/by-puuid/{puuid}/ids?start={cnt * 200}&count={MATCHES_COUNT}"
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()
                full_return_data += list(data)
            
            url = f"{BASE_URL}/tft/match/v1/matches/by-puuid/{puuid}/ids?start={(cnt + 1) * 200}&count={remain_api_call}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            full_return_data += list(data)

            if len(full_return_data) > 0:
                logging.info(f"Fetched {len(full_return_data)} matching IDs for puuid: {puuid}")
                return list(full_return_data)
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
    exe_datetime = kwargs['execution_date']  # execution_date 기준으로 폴더 명을 나눔
    exe_string = exe_datetime.strftime('%Y-%m-%d')
    full_matching_ids = dict(zip(puuid_list, [0] * len(puuid_list)))

    if puuid_list:
        for puuid in puuid_list:
            # puuid에 대해 matching ID를 가져옵니다
            time.sleep(1)
            matching_ids = get_matching_ids(puuid)
            full_matching_ids[exe_string + '_' + puuid + '_' + 'matching_id'] = matching_ids
            logging.info(f"Matching IDs for puuid {puuid}: {matching_ids}")
        
        table = pa.Table.from_pydict(full_matching_ids)
        file_name = exe_string + '/' + 'matching_ids' + '_' + exe_string + '.parquet'
        buffer = pa.BufferOutputStream()
        pq.write_table(table, buffer)
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=file_name,
            Body=buffer.getvalue(),
            ContentType='application/octet-stream' # Parquet 파일은 이진 형식이므로 이 MIME 타입을 사용하여 파일을 전송한다.
        )
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

        file_name = exe_string + '/' + match + '_' + exe_string + '.parquet'
        try:
            # json을 PyArrow로 변환 후 S3에 업로드
            table = pa.Table.from_pylist(data)
            buffer = pa.BufferOutputStream()
            pq.write_table(table, buffer)
            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=file_name,
                Body=buffer.getvalue(),
                ContentType='application/octet-stream' # Parquet 파일은 이진 형식이므로 이 MIME 타입을 사용하여 파일을 전송한다.
            )
            logging.info(f"Successfully uploaded {file_name} to S3.")
        except boto3.exceptions.Boto3Error as e:
            logging.error(f"Failed to upload {file_name} to S3: {e}")
    
    return exe_string

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
