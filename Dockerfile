
# Use the official Airflow image as the base
FROM apache/airflow:2.9.1

# Set environment variables (you can customize these)
ENV AIRFLOW__CORE__EXECUTOR=CeleryExecutor
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
ENV AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:airflow@postgres/airflow
ENV AIRFLOW__CELERY__BROKER_URL=redis://:@redis:6379/0
ENV AIRFLOW__CORE__FERNET_KEY=''
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION='true'
ENV AIRFLOW__CORE__LOAD_EXAMPLES='true'
ENV AIRFLOW__API__AUTH_BACKENDS='airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session'
ENV AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK='true'
ENV _PIP_ADDITIONAL_REQUIREMENTS=${_PIP_ADDITIONAL_REQUIREMENTS:-pandas}

# Install any additional dependencies if required
# RUN pip install pandas
# USER root

# Set airflow user password
# RUN echo "airflow:airflow" | chpasswd

# Copy custom setup scripts or files (like setup_vim.sh)
# COPY ./mysettings/setup_vim.sh /opt/airflow/mysettings/setup_vim.sh

# Make sure the script is executable
# RUN chmod +x /opt/airflow/mysettings/setup_vim.sh

# Set the command for the scheduler service (overrides the default entrypoint)
# CMD ["bash", "-c", "/opt/airflow/mysettings/setup_vim.sh && airflow scheduler"]

# dbt위한 git 설치
# USER root
# RUN apt-get update && apt-get install -y git && apt-get clean

# airflow 유저로 복귀
USER airflow

COPY mysettings/requirements.txt /opt/airflow/mysettings/requirements.txt
RUN pip install -r mysettings/requirements.txt

# Expose the port for health check if needed
EXPOSE 8974

# Optionally add your healthcheck command (this will be part of the docker-compose config)
HEALTHCHECK CMD curl --fail http://localhost:8974/health || exit 1
