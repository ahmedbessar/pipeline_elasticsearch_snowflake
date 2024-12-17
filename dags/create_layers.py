from airflow.providers.docker.operators.docker import DockerOperator
from airflow import DAG
from datetime import datetime, timedelta
from docker.types import Mount
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': 'ahmed.bisar@waseet.net',
    'email_on_retry': 'ahmed.bisar@waseet.net',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'create_layers_in_snowflake',
    start_date=datetime.now(),
    schedule_interval='0 12 * * *',
    catchup=False
)

# Paths
LOCAL_DBT_PATH = r'C:/Users/Abisar/pipline_waseet/Airflow_DBT_Snowflake_Docker/dbtlearn'
DOCKER_MOUNT_PATH = '/dbtlearn'

# Incremental Load Task 1: Bash Operator
incremental_load_bash = BashOperator(
    task_id='incremental_load_bash',  # Unique task_id
    bash_command='{{ "bash /opt/airflow/scripts/incremental_load.sh" }}',
    dag=dag
)

# Incremental Load Task 2: Python Operator
def incremental_load_func():
    import subprocess
    subprocess.run(["python", "/opt/airflow/scripts/incremental_load.py"], check=True)

incremental_load_python = PythonOperator(
    task_id='incremental_load_python',  # Unique task_id
    python_callable=incremental_load_func,
    dag=dag
)

# DBT Business Layer Task
run_dbt_business_layer = DockerOperator(
    task_id='create_business_layer',
    image='custom_dbt_image',
    api_version='auto',
    docker_url='unix://var/run/docker.sock',
    command=f'sh -c "cd {DOCKER_MOUNT_PATH} && dbt run --models mart_classified_reports.* --project-dir {DOCKER_MOUNT_PATH}"',
    mounts=[Mount(source=LOCAL_DBT_PATH, target=DOCKER_MOUNT_PATH, type='bind')],
    network_mode='container:dbt',
    dag=dag
)

# Task Dependencies
incremental_load_bash >> incremental_load_python >> run_dbt_business_layer


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
