import io

# PyArrow Table 생성
data = pa.Table.from_pydict({"name": ["Alice", "Bob"], "age": [25, 30]})

# 버퍼 생성 및 데이터 작성
buffer = io.BytesIO()
pq.write_table(data, buffer)

# 데이터 확인 또는 이후 작업
buffer.seek(0)  # 버퍼 포인터를 처음으로 이동
data_to_upload = buffer.getvalue()

# S3 업로드 또는 다른 작업
s3.put_object(Bucket="your-bucket-name", Key="your-key.parquet", Body=data_to_upload)
