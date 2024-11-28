import boto3
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow import DAG

# S3 업로드 함수 정의
def upload_to_s3():
    bucket_name = "tft-team2-rawdata"
    object_key = "test-upload.txt"
    s3 = boto3.client('s3')

    try:
        response = s3.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body="This is a test upload from Airflow!"
        )
        print(f"Upload successful: {response}")
    except Exception as e:
        print(f"Error occurred: {e}")
        raise

# DAG 정의
with DAG(
    dag_id="s3_upload_test_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # On-demand 실행
    catchup=False,
    tags=["example"],
) as dag:

    # S3 업로드 태스크 정의
    s3_upload_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3
    )

    s3_upload_task
