from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import os
from minio import Minio

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 19),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'crypto_data_generator',
    default_args=default_args,
    description='Generate hourly crypto data and upload to MinIO',
    schedule_interval='@hourly',
    catchup=False,
    max_active_runs=1,
)

def generate_and_upload_data(**context):
    """Generate crypto data and upload to MinIO with organized folder structure"""
    from scripts.generate_data import generate_csv_for_hour
    from scripts.upload_to_minio import upload_files
    
    # Generate data for current hour
    execution_date = context['execution_date']
    
    # Create organized folder structure: raw-data/YYYY/MM/DD/HH/
    folder_path = f"raw-data/{execution_date.strftime('%Y/%m/%d/%H')}"
    filename = f"crypto_data_{execution_date.strftime('%Y%m%d_%H')}.csv"
    
    # Generate data
    generate_csv_for_hour(execution_date, output_path=f"temp/{filename}")
    
    # Upload to MinIO with organized path
    upload_to_organized_path(f"temp/{filename}", f"{folder_path}/{filename}")
    
    return {"file_path": f"{folder_path}/{filename}", "records": "generated"}

generate_data_task = PythonOperator(
    task_id='generate_and_upload_crypto_data',
    python_callable=generate_and_upload_data,
    dag=dag,
)
