FROM apache/airflow:2.9.2-python3.11

# ENV AIRFLOW__WEBSERVER__SECRET_KEY=buiducnhan15122003

USER root
RUN apt update && \
    apt-get -y install git && \
    apt-get clean;

USER airflow

COPY ./requirements.txt .

RUN pip install --no-cache-dir -r ./requirements.txt