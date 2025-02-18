from airflow import DAG                                 # type: ignore
from airflow.utils.task_group import TaskGroup          # type: ignore
from airflow.operators.bash import BashOperator         # type: ignore
from airflow.operators.dummy import DummyOperator       # type: ignore
from airflow.utils.dates import days_ago                # type: ignore
from trino_operator import TrinoOperator                # type: ignore


with DAG(
    'DBT_Streamify_Transformations',
    schedule_interval='0 23 * * *',
    default_args={
        'start_date': days_ago(1),
        'email_on_failure': True,
        'email_on_success': True,
        'email_on_retry': True,
        'email': ['nhanbui15122003@gmail.com']
    },
    catchup=False
) as dag:
    ## Start and End tasks
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")
    
    with TaskGroup("validate_configs", tooltip="Tasks for validations before running") as dbt_validate:
        dbt_debug = BashOperator(
            task_id='debug_yaml_files',
            bash_command='cd /opt/airflow/core && /opt/airflow/dbt_env/bin/dbt debug',
        )
        dbt_source_check = BashOperator(
            task_id='check_data_sources',
            bash_command='cd /opt/airflow/core && /opt/airflow/dbt_env/bin/dbt source freshness',
        )
        
        [dbt_debug, dbt_source_check]
    
    dbt_install_packages = BashOperator(
        task_id='install_packages',
        bash_command='cd /opt/airflow/core && /opt/airflow/dbt_env/bin/dbt deps',
    )
    
    with TaskGroup("transform_data", tooltip="Tasks for transformations") as dbt_execute:
        refresh_data = TrinoOperator(
            task_id='refresh_delta_tables',
            trino_conn_id='trino-delta',
            sql="./sql/drop_tables.sql"
        )
        dbt_run = BashOperator(
            task_id='run_transformations',
            bash_command='cd /opt/airflow/core && /opt/airflow/dbt_env/bin/dbt run',
        )
        
        refresh_data >> dbt_run    


start >> dbt_validate >> dbt_install_packages >> dbt_execute >> end