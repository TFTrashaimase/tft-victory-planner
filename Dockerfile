FROM python:3.8-slim-buster
LABEL Maintainer="tft.rashaimase@gmail.com"
WORKDIR /opt/airflow/
COPY requirements.txt ./
RUN pip install -r requirements.txt
CMD

