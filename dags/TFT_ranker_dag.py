import logging
import requests
import boto3
import time
import pyarrow as pa
import pyarrow.parquet as pq
import io

from itertools import repeat
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # 나중에 필요함
from airflow.utils.task_group import TaskGroup  # 나중에 필요함

# Variables 읽어오기 - .env 참고
API_KEY = Variable.get("API_KEY", default_var=None)
BASE_URL = Variable.get("BASE_URL", default_var=None)
QUEUE_TYPE = Variable.get("QUEUE_TYPE", default_var=None)
BUCKET_NAME = Variable.get("BUCKET_NAME", default_var=None)
BUCKET_REGION = Variable.get("BUCKET_REGION", default_var=None)
AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY", default_var=None)
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_KEY", default_var=None)

# s3 버킷 설정
s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name='ap-northeast-2')  # 서울 리전 

# 상수 정의
tiers = ["DIAMOND", "EMERALD", "PLATINUM", "GOLD", "SILVER", "BRONZE"]
divisions = ["I", "II", "III", "IV"]
page = 1
MATCHES_COUNT = 20  # 한 번에 가져올 매치 수, 원한다면 수정해서 작업. 

# Variables가 없다면 에러 발생
if not API_KEY:
    raise ValueError("환경 변수 API_KEY의 확인이 필요합니다.")

if not BASE_URL:
    raise ValueError("환경 변수 BASE_URL의 확인이 필요합니다.")

if not QUEUE_TYPE:
    raise ValueError("환경 변수 QUEUE_TYPE의 확인이 필요합니다.")

if not BUCKET_NAME: 
    raise ValueError("환경 변수 BUCKET_NAME의 확인이 필요합니다.")

if not BUCKET_REGION:
    raise ValueError("환경 변수 BUCKET_REGION의 확인이 필요합니다.")

if not AWS_ACCESS_KEY_ID:
    raise ValueError("환경 변수 AWS_ACCESS_KEY_ID의 확인이 필요합니다.")

if not AWS_SECRET_ACCESS_KEY:
    raise ValueError("환경 변수 AWS_SECRET_ACCESS_KEY의 확인이 필요합니다.")

# Summoner ID로 유저 데이터를 수집하는 함수 - 챌린저, 그랜드 마스터, 마스터
def get_entries_by_summoner(summoner_id, **kwargs):
    url = f"{BASE_URL}/tft/league/v1/entries/by-summoner/{summoner_id}"

    try:
        response = requests.get(url, headers={"X-Riot-Token": API_KEY})
        response.raise_for_status()
        data = response.json()
        if data:
            logging.info(f"Fetched data for Summoner ID {summoner_id}: {data}")
            return data[0]
        else:
            logging.info(f"No entries found for Summoner ID {summoner_id}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data for Summoner ID {summoner_id}: {e}")
        raise

# 챌린저 유저 SummonerId를 받아와서 다른 api를 통해 puuid를 포함한 유저 데이터를 반환
def get_challenger(**kwargs):
    url = f"{BASE_URL}/tft/league/v1/challenger?queue={QUEUE_TYPE}"
    try:
        response = requests.get(url, headers={"X-Riot-Token": API_KEY})
        response.raise_for_status()
        summoner_ids = response.json()["entries"]
        challenger_data = []
        if len(summoner_ids) > 0:
            for challenger_info in summoner_ids:
                time.sleep(1)
                s_id = challenger_info["summonerId"]

                challenger_data.append(get_entries_by_summoner(s_id))
            if len(challenger_data) > 0:
                return challenger_data
            else:
                logging.info("No challenger players found")
                return None
        else:
            logging.info("No Challenger players found")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching Challenger data: {e}")
        return None

# 그랜드 마스터 유저 SummonerId를 받아와서 다른 api를 통해 puuid를 포함한 유저 데이터를 반환
def get_grandmaster(**kwargs):
    url = f"{BASE_URL}/tft/league/v1/grandmaster?queue={QUEUE_TYPE}"
    try:
        response = requests.get(url, headers={"X-Riot-Token": API_KEY})
        response.raise_for_status()
        summoner_ids = response.json()["entries"]
        gmaster_data = []
        if len(summoner_ids) > 0:
            for gmaster_info in summoner_ids:
                s_id = gmaster_info["summonerId"]

                gmaster_data.append(get_entries_by_summoner(s_id))
            if len(gmaster_data) > 0:
                return gmaster_data
            else:
                logging.info("No Grandmaster players found")
                return None
        else:
            logging.info("No Grandmaster players found")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching Grandmaster data: {e}")
        return None

# 마스터 유저 SummonerId를 받아와서 다른 api를 통해 puuid를 포함한 유저 데이터를 반환
def get_master(**kwargs):
    url = f"{BASE_URL}/tft/league/v1/master?queue={QUEUE_TYPE}"
    try:
        response = requests.get(url, headers={"X-Riot-Token": API_KEY})
        response.raise_for_status()
        summoner_ids = response.json()["entries"]
        master_data = []
        if len(summoner_ids) > 0:
            for master_info in summoner_ids:
                s_id = master_info["summonerId"]

                master_data.append(get_entries_by_summoner(s_id))
            if len(master_data) > 0 :
                return master_data
            else:
                logging.info("No Master players found")
                return None
        else:
            logging.info("No Master players found")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching Master data: {e}")
        return None

# 챌린저, 그랜드 마스터, 마스터까지 상위 랭크 유저가 100명이 되지 않는다면 100명 이상의 정보를 모으기 위해 그 아래 티어를 호출
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

# 상위 랭커들의 puuid 포함한 정보를 합쳐서 하나의 리스트로 반환
# 이 과정에서 100 + 알파 명의 유저의 정보 역시 S3에 parquet으로 업로드
def process_puuid_data(**kwargs):
    ti = kwargs['ti']
    challenger_data = ti.xcom_pull(task_ids='get_challenger_task')
    grandmaster_data = ti.xcom_pull(task_ids='get_grandmaster_task')
    master_data = ti.xcom_pull(task_ids='get_master_task')
    tier_data = ti.xcom_pull(task_ids='get_tier_task')

    raw_puuid_data = (challenger_data if challenger_data else []) + (grandmaster_data if grandmaster_data else []) \
        + (master_data if master_data else []) + (tier_data if tier_data else [])

    exe_datetime = kwargs['execution_date']  # execution_date 기준으로 폴더 명을 나눔
    exe_string = exe_datetime.strftime('%Y-%m-%d')

    result_dict = dict()

    for key in raw_puuid_data[0]:
        for d in raw_puuid_data:
            if key not in result_dict:
                result_dict[key] = [d[key]]
            else:
                result_dict[key].append(d[key])
    
    table = pa.Table.from_pydict(result_dict)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)  # 버퍼 포인터를 처음으로 이동
    buffer_writer = buffer.getvalue()

    # 3. S3 업로드
    file_name = exe_string + '/' + 'puuid' + '_' + exe_string + '.parquet'
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=file_name,
        Body=bytes(buffer_writer),
        ContentType='application/octet-stream'  # Parquet 파일에 적합한 MIME 타입
    )

    return raw_puuid_data

# 상위 랭커들의 puuid 기반으로 API를 호출하여 Matching ID 목록을 가져오는 함수
def get_matching_ids(puuid, **kwargs):
    url = f"https://asia.api.riotgames.com/tft/match/v1/matches/by-puuid/{puuid}/ids?start=0&count={MATCHES_COUNT}"
    request_header = {
        "X-Riot-Token": API_KEY
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
        

# 딕셔너리 길이를 맞추어 주는 helper function
def pad_dict_to_max_length(data_dict, pad_value=None):
    # 최대 길이 계산
    max_length = max(len(lst) for lst in data_dict.values())
    
    # 각 리스트를 최대 길이로 패딩
    padded_dict = {
        key: lst + list(repeat(pad_value, max_length - len(lst)))
        for key, lst in data_dict.items()
    }
    
    return padded_dict


# matching id를 하나의 리스트로 정리
def process_matching_ids(**kwargs):
    # TFT_ranker_dag.py의 process_data_task에서 puuid_list를 가져옵니다 (XCom을 사용)
    puuid_list = kwargs['ti'].xcom_pull(task_ids='process_puuid_raw_data')  # 'process_data_task' 사용
    exe_datetime = kwargs['execution_date']  # execution_date 기준으로 폴더 명을 나눔
    exe_string = exe_datetime.strftime('%Y-%m-%d')
    full_matching_ids = dict(zip([user['puuid'] for user in puuid_list], [[]] * len(puuid_list)))

    if puuid_list:
        for user in puuid_list:
            puuid = user['puuid']
            # puuid에 대해 matching ID를 가져옵니다
            time.sleep(1)
            matching_ids = get_matching_ids(puuid)
            full_matching_ids[exe_string + '_' + puuid + '_' + 'matching_id'] = matching_ids
            logging.info(f"Matching IDs for puuid {puuid}: {matching_ids}")
        
        full_matching_ids = pad_dict_to_max_length(full_matching_ids)

        table = pa.Table.from_pydict(full_matching_ids)
        file_name = exe_string + '/' + 'matching_ids' + '_' + exe_string + '.parquet'
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)  # 버퍼 포인터를 처음으로 이동
        buffer_writer = buffer.getvalue()
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=file_name,
            Body=bytes(buffer_writer),
            ContentType='application/octet-stream' # Parquet 파일은 이진 형식이므로 이 MIME 타입을 사용하여 파일을 전송한다.
        )
    else:
        logging.error(f"{puuid}: No match data for this puuid found from process_data_task in TFT_ranker_dag.py")

    return full_matching_ids

# 값들의 길이를 동일하게 맞추는 헬퍼 함수
def ensure_uniform_length(data, target_length):
    if isinstance(data, list):
        return data + list(repeat(None, target_length - len(data)))
    elif isinstance(data, dict):
        return [data] + list(repeat(None, target_length - 1))
    else:
        return [data] + list(repeat(None, target_length - 1))

# 모든 값을 리스트로 변환하고 길이를 동일하게 맞춤
# 딕셔너리, 그냥 스칼라는 그냥 리스트에 넣고 None을 길이에 맞추어 패딩
# 리스트는 최대 길이에 맞춰 None을 넣어 패딩
def normalize_json(data):
    # 모든 값을 리스트로 변환
    for key in data:
        if not isinstance(data[key], list):
            data[key] = [data[key]]  # 리스트로 변환

    # 최대 길이 계산
    max_length = max(len(value) for value in data.values())

    # 모든 값을 동일한 길이로 맞춤
    for key in data:
        data[key] = ensure_uniform_length(data[key], max_length)

    return data

# 중첩 JSON 처리
def process_nested_json(data):
    """
    중첩된 JSON 데이터를 처리합니다.
    """
    if isinstance(data, dict):
        return {k: process_nested_json(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [process_nested_json(item) if isinstance(item, (dict, list)) else item for item in data]
    else:
        return data

# s3://tft-team2-rawdata/match_infos/{YYYY-MM-DD}/{puuid}_{match_id}.parquet
# s3에 적재
def matching_info_to_s3(**kwargs):
    exe_datetime = kwargs['execution_date']  # execution_date 기준으로 폴더 명을 나눔
    exe_string = exe_datetime.strftime('%Y-%m-%d')
    match_data = kwargs['ti'].xcom_pull(task_ids='get_matching_ids_task')  # 매치데이터를 받아옴

    if not match_data:
        logging.info("No match data to process")
        return exe_string

    # API 호출 제한 => 1초에 20번, 2분에 100번
    for user in match_data.keys():
        time.sleep(0.5)
        matches = match_data[user]

        for match in matches:
            time.sleep(1.5)
            if match is None:
                continue
            time.sleep(0.5)
            match_api_url = f"https://asia.api.riotgames.com/tft/match/v1/matches/{match}"
            response = requests.get(match_api_url, headers={"X-Riot-Token": API_KEY})
            response.raise_for_status()
            data = response.json()

            # s3://tft-team2-rawdata/match_infos/{YYYY-MM-DD}/{puuid}_{match_id}.parquet
            file_name = 'tft-team2-rawdata/match_infos/' + exe_string + '/' + user + '_' + match + '.parquet'
            try:
                # json을 PyArrow로 변환 후 S3에 업로드
                data = normalize_json(process_nested_json(data))
                table = pa.Table.from_pydict(data)
                buffer = io.BytesIO()
                pq.write_table(table, buffer)
                buffer.seek(0)  # 버퍼 포인터를 처음으로 이동
                buffer_writer = buffer.getvalue()
                s3.put_object(
                    Bucket=BUCKET_NAME,
                    Key=file_name,
                    Body=bytes(buffer_writer),
                    ContentType='application/octet-stream' # Parquet 파일은 이진 형식이므로 이 MIME 타입을 사용하여 파일을 전송한다.
                )
                logging.info(f"Successfully uploaded {file_name} to S3.")
            except boto3.exceptions.Boto3Error as e:
                logging.error(f"Failed to upload {file_name} to S3: {e}")
    
    return exe_string

# DAG 기본 설정
default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'TFT_Riot_API_Dag',
    default_args=default_args,
    description='Get data from Riot API for TFT rankings',
    schedule_interval='0 0 * * *',  # 매일 자정에 실행
    catchup=False,  # 과거 실행 날짜에 대해 실행하지 않음
    tags=['riot', 'tft']
) as dag:

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
    matching_info_to_s3_task = PythonOperator(
        task_id='matching_info_to_s3_task',
        python_callable=matching_info_to_s3,
        provide_context=True,
        dag=dag
    )

    # 트리거할 DAG ID 리스트
    dag_ids_to_trigger = ['snowflake_load_dag']

    with TaskGroup(group_id='trigger_snowflake_load_dags') as trigger_group:
        for dag_id in dag_ids_to_trigger:
            conf = {
                    "s3_bucket_folder": "{{ task_instance.xcom_pull(task_ids='load_json_to_s3') }}",
                    "triggered_by": "TFT_Riot_API_Dag", 
                    "triggered_dag": dag_id,
                    "execution_date": "{{ execution_date.isoformat() }}",
                }
            TriggerDagRunOperator(task_id=f'trigger_{dag_id}', trigger_dag_id=dag_id, conf=conf)


# 태스크 의존성 설정
[challenger_task, grandmaster_task, master_task, tier_task] >> process_puuid >> matching_id_task >> matching_info_to_s3_task >> trigger_group