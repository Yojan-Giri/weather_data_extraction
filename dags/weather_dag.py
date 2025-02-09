import os
import sys
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.s3_upload_pipeline import upload_data_s3_pipeline
from pipelines.open_weather_pipeline import extract_open_weather_pipeline

# DEFINE DEFAULT ARGUMENTS
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 5),
    "email": ['yojangiri@gmail.com'],
    "email_on_failure": False,  
    "email_on_retry": False,     
    "retries": 0,
}

# DEFINE DAG
dag = DAG(
    "w_data_dag",
    default_args=default_args,
    schedule_interval=None,  # Set the interval as needed
    catchup=False,
)

extract_weather_data=PythonOperator(
    task_id="extract_weather_data",
    python_callable=extract_open_weather_pipeline,
    dag=dag
)

# Define Tasks
upload_data_file_to_s3 = PythonOperator(
    task_id='s3_data_upload',
    python_callable=upload_data_s3_pipeline,
    dag=dag
)




# Set Task Dependencies
extract_weather_data >> upload_data_file_to_s3 