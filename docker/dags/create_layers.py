from airflow.providers.docker.operators.docker import DockerOperator
from airflow import DAG
from datetime import datetime
from datetime import datetime, timedelta
from docker.types import Mount

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': 'ahmed.bisar@waseet.net',
    'email_on_retry': 'ahmed.bisar@waseet.net',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Define the DAG
dag = DAG(
    'create_layers_in_snowflake',
    start_date=datetime.now(),
    schedule_interval='0 12 * * *',
    catchup=False
)

# Local path to DBT project
LOCAL_DBT_PATH = r'C:/Users/Abisar/pipline_waseet/Airflow_DBT_Snowflake_Docker/dbtlearn'

# Convert the local path to Docker-compatible format
DOCKER_MOUNT_PATH = '/dbtlearn'


run_dbt_business_layer = DockerOperator(
    task_id='create_business_layer',
    image='custom_dbt_image',
    api_version='auto',
    docker_url='unix://var/run/docker.sock',
    command=f'sh -c "cd {DOCKER_MOUNT_PATH} && dbt run --mart_classified_reports.* --project-dir {DOCKER_MOUNT_PATH}"',
    mounts=[Mount(source=LOCAL_DBT_PATH, target=DOCKER_MOUNT_PATH, type='bind')],
    network_mode='container:dbt',
    dag=dag
)


# run_dbt_docs = DockerOperator(
#     task_id='create_lineage_graph',
#     image='custom_dbt_image',
#     api_version='auto',
#     docker_url='unix://var/run/docker.sock',
#     command=f'sh -c "cd {DOCKER_MOUNT_PATH} && dbt docs serve --port 8085"',
#     mounts=[Mount(source=LOCAL_DBT_PATH, target=DOCKER_MOUNT_PATH, type='bind')],
#     network_mode='container:dbt',
#     dag=dag
# )

# Task Dependencies
run_dbt_business_layer 