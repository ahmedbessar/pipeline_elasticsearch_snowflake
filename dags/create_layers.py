from airflow.providers.docker.operators.docker import DockerOperator
from airflow import DAG
from datetime import datetime, timedelta
from docker.types import Mount
from airflow.operators.python_operator import PythonOperator
import os
import subprocess
import sys

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': 'ahmed.bisar@waseet.net',
    'email_on_retry': 'ahmed.bisar@waseet.net',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'create_layers_in_snowflake',
    default_args=default_args,
    description='Run incremental loads in parallel and DBT business layer',
    schedule_interval='0 12 * * *',
    start_date=datetime(2024, 1, 1),
    concurrency=1,
    catchup=False,
)

# Wrapper function to execute the script
def run_extraction_load_script():
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

# Paths
LOCAL_DBT_PATH = r'C:/Users/Abisar/pipline_waseet/Airflow_DBT_Snowflake_Docker/dbtlearn'  # Ensure this is accessible in the Airflow environment
DOCKER_MOUNT_PATH = '/dbtlearn'

# Task to run the extraction and load script
incremental_product_task = PythonOperator(
    task_id='run_extraction_and_load',
    python_callable=run_extraction_load_script,
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

# Define dependencies
incremental_product_task >> run_dbt_business_layer


# # Incremental Load Task 1
# def incremental_product_post_kw():
#     import subprocess
#     subprocess.run(["python", "/opt/airflow/scripts/Incremental__product_post_kw_snowflake.py"], check=True)

# incremental_product_task = PythonOperator(
#     task_id='incremental_product_post_kw',
#     python_callable=incremental_product_post_kw,
#     dag=dag
# )

# # Incremental Load Task 2
# def incremental_classified_log_report():
#     import subprocess
#     subprocess.run(["python", "/opt/airflow/scripts/incremental_classified_log_report_els_to_snowflake.py"], check=True)

# incremental_classified_task = PythonOperator(
#     task_id='incremental_classified_log_report',
#     python_callable=incremental_classified_log_report,
#     dag=dag
# )



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
