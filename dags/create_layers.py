from airflow.providers.docker.operators.docker import DockerOperator
from airflow import DAG
from datetime import datetime, timedelta
from docker.types import Mount
from airflow.operators.python_operator import PythonOperator
import os
import subprocess
import sys
import logging
from airflow.utils.dates import timezone  # Airflow's timezone utility for datetime handling
import pytz  # For explicit timezone definitions if needed

# Define timezone
EET = pytz.timezone("EET")

# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['ahmed.bisar@waseet.net'],  # Email to notify
    'email_on_failure': True,  # Notify on failure
    'retries': 3,  # Retry up to 3 times
    'retry_delay': timedelta(minutes=5),  # Wait 5 minutes between retries
}

# Define the DAG
dag = DAG(
    'create_business_layer_snowflake',
    default_args=default_args,
    description='Run incremental loads in parallel and DBT business layer',
    schedule_interval='0 */3 * * *',  # Run every 3 hours
    start_date=timezone.datetime(2025, 1, 20, 5, 0, tzinfo=EET), # Use pytz for timezone-aware datetime
    concurrency=1,  # Limit to one task at a time
    catchup=False,
)

# Wrapper function to execute the script
def Classified_Log_KW():
    script_path = '/opt/airflow/scripts/inc_classified_log_kw2sf.py'
    if os.path.exists(script_path):
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            raise RuntimeError(f"Script failed with output: {result.stderr}")
    else:
        raise FileNotFoundError(f"{script_path} does not exist.")

def Classified_Log_All():
    script_path = '/opt/airflow/scripts/inc_classified_log_all2sf.py'
    if os.path.exists(script_path):
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            raise RuntimeError(f"Script failed with output: {result.stderr}")
    else:
        raise FileNotFoundError(f"{script_path} does not exist.")
    
def Prod_Post_All():
    script_path = '/opt/airflow/scripts/inc_prod_post_all2sf.py'
    if os.path.exists(script_path):
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            raise RuntimeError(f"Script failed with output: {result.stderr}")
    else:
        raise FileNotFoundError(f"{script_path} does not exist.")

def Prod_Post_KW():
    script_path = '/opt/airflow/scripts/inc_prod_post_kw2sf.py'
    if os.path.exists(script_path):
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            raise RuntimeError(f"Script failed with output: {result.stderr}")
    else:
        raise FileNotFoundError(f"{script_path} does not exist.")
    
# Paths
LOCAL_DBT_PATH = r'C:/Users/Abisar/pipline_waseet/Airflow_DBT_Snowflake_Docker/dbtlearn'  # Ensure this is accessible in the Airflow environment
DOCKER_MOUNT_PATH = '/dbtlearn'

# Task to run the extraction and load script
task_classified_log_kw = PythonOperator(
    task_id='run_classified_log_kw',
    python_callable=Classified_Log_KW,
    # retry_delay=timedelta(minutes=5),
    dag=dag
)
task_classified_log_all = PythonOperator(
    task_id='run_classified_log_all',
    python_callable=Classified_Log_All,
    # retry_delay=timedelta(minutes=5),
    dag=dag
)
task_prod_post_all = PythonOperator(
    task_id='run_prod_post_all',
    python_callable=Prod_Post_All,
    # retry_delay=timedelta(minutes=5),
    dag=dag
)
task_prod_post_kw = PythonOperator(
    task_id='run_prod_post_kw',
    python_callable=Prod_Post_KW,
    # retry_delay=timedelta(minutes=5),
    dag=dag
)

# DBT Business Layer Task
run_dbt_business_layer = DockerOperator(
    task_id='create_business_layer',
    image='custom_dbt_image',
    api_version='auto',
    docker_url='unix://var/run/docker.sock',
    command=f'sh -c "cd {DOCKER_MOUNT_PATH} && dbt run --models mart_classified.* --project-dir {DOCKER_MOUNT_PATH}"',
    mounts=[Mount(source=LOCAL_DBT_PATH, target=DOCKER_MOUNT_PATH, type='bind')],
    network_mode='bridge',  # Change if needed
    dag=dag
)

run_dbt_docs = DockerOperator(
    task_id='create_lineage_graph',
    image='custom_dbt_image',
    api_version='auto',
    docker_url='unix://var/run/docker.sock',
    command=f'sh -c "cd {DOCKER_MOUNT_PATH} && dbt docs generate && (dbt docs serve --port 8085 &) && sleep 30"',
    mounts=[Mount(source=LOCAL_DBT_PATH, target=DOCKER_MOUNT_PATH, type='bind')],
    network_mode='container:dbt',
    dag=dag
)


# Define dependencies
[task_classified_log_kw, task_classified_log_all, task_prod_post_all, task_prod_post_kw] >> run_dbt_business_layer >> run_dbt_docs
