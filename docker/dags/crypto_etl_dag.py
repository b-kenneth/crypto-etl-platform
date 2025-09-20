from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
import sys
import os

import etl.logger_config

# Add project root to Python path
sys.path.append('/opt/airflow')

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 19),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'crypto_etl_main',
    default_args=default_args,
    description='Robust crypto data ETL pipeline with quality checks and notifications',
    schedule_interval=timedelta(minutes=15),
    catchup=False,
    max_active_runs=1,
)

def check_new_files(**context):
    """Check for unprocessed files in MinIO"""
    from etl.extract import MinioExtractor
    from etl.file_processor import FileProcessingManager
    
    extractor = MinioExtractor()
    processor = FileProcessingManager()
    
    all_files = extractor.list_files(prefix="raw-data/")
    unprocessed_files = processor.get_unprocessed_files(all_files)
    
    if not unprocessed_files:
        print("No new files to process")
        return 'no_new_files'
    
    print(f"Found {len(unprocessed_files)} new files to process")
    context['task_instance'].xcom_push(key='files_to_process', value=unprocessed_files)
    return 'validate_files'

def validate_file_structure(**context):
    """Validate structure of all unprocessed files"""
    from etl.extract import MinioExtractor
    from etl.data_quality import DataQualityChecker
    from etl.file_processor import FileProcessingManager
    
    extractor = MinioExtractor()
    checker = DataQualityChecker()
    processor = FileProcessingManager()
    
    files_to_process = context['task_instance'].xcom_pull(key='files_to_process')
    if not files_to_process:
        print("No files to validate")
        return "No files received for validation"
    
    valid_files = []
    invalid_files = []
    
    for file_path in files_to_process:
        try:
            processor.mark_file_processing(file_path, 0)
            df = extractor.read_csv(file_path)
            is_valid, errors = checker.validate_file_structure(df)
            
            if is_valid:
                valid_files.append({
                    'file_path': file_path,
                    'record_count': len(df)
                })
                print(f"✓ Valid structure: {file_path} ({len(df)} records)")
            else:
                invalid_files.append({
                    'file_path': file_path,
                    'errors': errors
                })
                processor.mark_file_failed(file_path, f"Structure validation failed: {'; '.join(errors)}")
                print(f"✗ Invalid structure: {file_path} - {errors}")
                
        except Exception as e:
            invalid_files.append({
                'file_path': file_path,
                'errors': [str(e)]
            })
            processor.mark_file_failed(file_path, f"Structure validation error: {str(e)}")
            print(f"✗ Error validating {file_path}: {e}")
    
    context['task_instance'].xcom_push(key='valid_files', value=valid_files)
    context['task_instance'].xcom_push(key='invalid_files', value=invalid_files)
    
    if not valid_files:
        raise ValueError("No valid files found - all files failed validation")
    
    return f"Validated {len(valid_files)} valid files, {len(invalid_files)} invalid files"

def extract_and_quality_check(**context):
    """Extract data and perform comprehensive quality checks"""
    from etl.extract import MinioExtractor
    from etl.data_quality import DataQualityChecker
    from etl.file_processor import FileProcessingManager
    
    extractor = MinioExtractor()
    checker = DataQualityChecker()
    processor = FileProcessingManager()
    
    valid_files = context['task_instance'].xcom_pull(key='valid_files')
    if not valid_files:
        raise ValueError("No valid files received for processing")
    
    processed_data = []
    quality_failures = []
    
    for file_info in valid_files:
        file_path = file_info['file_path']
        
        try:
            df = extractor.read_csv(file_path)
            passed, issues, metrics = checker.validate_data_quality(df)
            
            if passed:
                processed_data.append({
                    'file_path': file_path,
                    'data': df.to_dict('records'),
                    'metrics': metrics
                })
                print(f"✓ Quality check passed: {file_path} - {metrics}")
            else:
                quality_failures.append({
                    'file_path': file_path,
                    'issues': issues
                })
                processor.mark_file_failed(file_path, f"Quality validation failed: {'; '.join(issues)}")
                print(f"✗ Quality check failed: {file_path} - {issues}")
                
        except Exception as e:
            quality_failures.append({
                'file_path': file_path,
                'issues': [str(e)]
            })
            processor.mark_file_failed(file_path, f"Quality check error: {str(e)}")
            print(f"✗ Error processing {file_path}: {e}")
    
    context['task_instance'].xcom_push(key='processed_data', value=processed_data)
    context['task_instance'].xcom_push(key='quality_failures', value=quality_failures)
    
    if not processed_data:
        raise ValueError("No files passed quality checks")
    
    return f"Quality checked {len(processed_data)} files successfully"

def transform_and_load_data(**context):
    """Transform data and load to database"""
    import pandas as pd
    from etl.transform import transform_data
    from etl.load import upsert_prices
    from etl.file_processor import FileProcessingManager
    
    processor = FileProcessingManager()
    processed_data = context['task_instance'].xcom_pull(key='processed_data')
    
    if not processed_data:
        raise ValueError("No processed data received for transform/load")
    
    total_records = 0
    successful_files = []
    failed_files = []
    
    for file_info in processed_data:
        file_path = file_info['file_path']
        
        try:
            df = pd.DataFrame(file_info['data'])
            transformed_df = transform_data(df)
            upsert_prices(transformed_df)
            processor.mark_file_completed(file_path, len(transformed_df))
            
            successful_files.append({
                'file_path': file_path,
                'record_count': len(transformed_df)
            })
            total_records += len(transformed_df)
            
            print(f"✓ Successfully processed: {file_path} ({len(transformed_df)} records)")
            
        except Exception as e:
            processor.mark_file_failed(file_path, f"Transform/load error: {str(e)}")
            failed_files.append({
                'file_path': file_path,
                'error': str(e)
            })
            print(f"✗ Failed to process: {file_path} - {e}")
    
    context['task_instance'].xcom_push(key='successful_files', value=successful_files)
    context['task_instance'].xcom_push(key='failed_files', value=failed_files)
    context['task_instance'].xcom_push(key='total_records', value=total_records)
    
    return f"Processed {len(successful_files)} files successfully, {total_records} total records"

def send_success_notification(**context):
    """Send success notification with processing summary"""
    successful_files = context['task_instance'].xcom_pull(key='successful_files') or []
    failed_files = context['task_instance'].xcom_pull(key='failed_files') or []
    total_records = context['task_instance'].xcom_pull(key='total_records') or 0
    
    message = f"""
    ETL Pipeline Execution Summary:
    
    ✅ Successfully processed: {len(successful_files)} files
    ❌ Failed: {len(failed_files)} files  
    📊 Total records processed: {total_records}
    
    Execution Date: {context['execution_date']}
    DAG: {context['dag'].dag_id}
    """
    
    print(message)
    return message

def send_failure_notification(**context):
    """Send detailed failure notification"""
    message = f"""
    ❌ ETL Pipeline Failed:
    
    Execution Date: {context['execution_date']}
    DAG: {context['dag'].dag_id}
    
    Please check logs for detailed error information.
    """
    
    print(message)
    return message

# Define tasks
start_task = DummyOperator(task_id='start', dag=dag)

check_files_task = BranchPythonOperator(
    task_id='check_new_files',
    python_callable=check_new_files,
    dag=dag,
)

no_files_task = DummyOperator(task_id='no_new_files', dag=dag)

validate_structure_task = PythonOperator(
    task_id='validate_files',
    python_callable=validate_file_structure,
    dag=dag,
)

extract_quality_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_and_quality_check,
    dag=dag,
)

transform_load_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_and_load_data,
    dag=dag,
)

success_notification_task = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_success_notification,
    dag=dag,
)

failure_notification_task = PythonOperator(
    task_id='send_failure_notification',
    python_callable=send_failure_notification,
    trigger_rule=TriggerRule.ONE_FAILED,
    dag=dag,
)

end_task = DummyOperator(
    task_id='end',
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag,
)

# Define task dependencies - Linear flow with error handling
start_task >> check_files_task

check_files_task >> [no_files_task, validate_structure_task]

validate_structure_task >> extract_quality_task >> transform_load_task >> success_notification_task

[validate_structure_task, extract_quality_task, transform_load_task] >> failure_notification_task

[no_files_task, success_notification_task, failure_notification_task] >> end_task

