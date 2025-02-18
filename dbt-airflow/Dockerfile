FROM apache/airflow:2.9.2-python3.11

USER root

RUN apt update && \
    apt-get -y install git && \
    apt-get clean;

USER airflow

# default environment for Trino Hook
COPY ./requirements.txt .
RUN pip install --no-cache-dir -r ./requirements.txt

# virtual environment for dbt
RUN python -m venv /opt/dbt_venv
RUN /opt/dbt_venv/bin/pip install dbt-core dbt-trino