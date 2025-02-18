# Introduction

Developed a Lakehouse-based data pipeline using Sakila dataset to analyze movie sales and rentals. The lakehouse was designed according to `Delta` architecture

- Extracted events in database by using CDC (Debezium), then published events to Kafka which ensures `scalable and fault-tolerant` message processing
- Processed streaming event by Spark Streaming and writes to Delta Tables in MinIO, combining with Trino query engine to provide `real-time insights` via Superset dashboards
- Transformed periodically event data in Delta Tables into staging and mart tables for `deep analytics and machine learning` using DBT

![image](https://github.com/user-attachments/assets/cc379c24-93b1-4a58-b719-d70221026769)

# Setup platforms
## Apache Spark cluster
Config files in these folders: `spark`, `notebook`, `hive-metastore`

Run this command to create Docker containers of Apache Spark cluster
```bash
docker-compose up -f ./docker-compose.yaml
```

## Apache Kafka
Config files in this folders: `kafka`

Run this command to create Apache Kafka's containers
```bash
docker-compose up -f ./kafka/docker-compose.yaml
```

## Trino and Superset
Config files in this folder: `trino-superset`

In **trino-superset/trino-conf/catalog**, create `delta.properties` with following parameters
```properties
connector.name=delta-lake
hive.metastore.uri=thrift://160.191.244.13:9083
hive.s3.aws-access-key=minio
hive.s3.aws-secret-key=minio123
hive.s3.endpoint=http://160.191.244.13:9000
hive.s3.path-style-access=true
```

Then run this command
```
docker-compose up --build
```

## Apache Airflow
> Note: this version is sequential not parallel

Config files in this folder: `dbt-airlow`

In **dbt-airflow**, run this command to create Airflow container

```
docker-compose up --build
```

# Run project

To start streaming process, run this command on `jupyter notebook's terminal` which is running in spark-notebook container
```
python3 stream_events.py
```

![image](https://github.com/user-attachments/assets/52fa73fa-1906-42c1-8d77-2d3cff33d216)


To run data warehouse's transformations, just need trigger this DAG in Airflow's UI or it will automatically run daily at `23:00 PM`

![image](https://github.com/user-attachments/assets/50f9aff8-4823-4bb9-b83c-3539e22e6462)




