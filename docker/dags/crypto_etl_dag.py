from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.email import EmailOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
import sys
import os

# Add project root to Python path
sys.path.append('/opt/airflow')

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 19),
    'email_on_failure': False,  # We'll handle notifications manually
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'crypto_etl_main',
    default_args=default_args,
    description='Robust crypto data ETL pipeline with quality checks and notifications',
    schedule_interval=timedelta(minutes=15),  # Check every 15 minutes
    catchup=False,
    max_active_runs=1,
)

def check_new_files(**context):
    """Check for unprocessed files in MinIO"""
    from etl.extract import MinioExtractor
    from etl.file_processor import FileProcessingManager
    
    extractor = MinioExtractor()
    processor = FileProcessingManager()
    
    # Get all files from MinIO
    all_files = extractor.list_files(prefix="raw-data/")
    
    # Filter to unprocessed files
    unprocessed_files = processor.get_unprocessed_files(all_files)
    
    if not unprocessed_files:
        print("No new files to process")
        return 'no_new_files'
    
    print(f"Found {len(unprocessed_files)} new files to process")
    
    # Store file list in XCom for downstream tasks
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
    valid_files = []
    invalid_files = []
    
    for file_path in files_to_process:
        try:
            # Mark as processing
            processor.mark_file_processing(file_path, 0)  # We'll update size later
            
            # Read and validate structure
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
    
    # Store results in XCom
    context['task_instance'].xcom_push(key='valid_files', value=valid_files)
    context['task_instance'].xcom_push(key='invalid_files', value=invalid_files)
    
    if not valid_files:
        return 'handle_validation_failure'
    elif invalid_files:
        return 'partial_validation_success'
    else:
        return 'extract_data'

def extract_and_quality_check(**context):
    """Extract data and perform comprehensive quality checks"""
    from etl.extract import MinioExtractor
    from etl.data_quality import DataQualityChecker
    from etl.file_processor import FileProcessingManager
    
    extractor = MinioExtractor()
    checker = DataQualityChecker()
    processor = FileProcessingManager()
    
    valid_files = context['task_instance'].xcom_pull(key='valid_files')
    processed_data = []
    quality_failures = []
    
    for file_info in valid_files:
        file_path = file_info['file_path']
        
        try:
            # Extract data
            df = extractor.read_csv(file_path)
            
            # Perform quality checks
            passed, issues, metrics = checker.validate_data_quality(df)
            
            if passed:
                processed_data.append({
                    'file_path': file_path,
                    'data': df.to_dict('records'),  # Convert to serializable format
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
    
    # Store results
    context['task_instance'].xcom_push(key='processed_data', value=processed_data)
    context['task_instance'].xcom_push(key='quality_failures', value=quality_failures)
    
    if not processed_data:
        return 'handle_quality_failure'
    else:
        return 'transform_data'

def transform_and_load_data(**context):
    """Transform data and load to database"""
    import pandas as pd
    from etl.transform import transform_data
    from etl.load import upsert_prices
    from etl.file_processor import FileProcessingManager
    
    processor = FileProcessingManager()
    processed_data = context['task_instance'].xcom_pull(key='processed_data')
    
    total_records = 0
    successful_files = []
    failed_files = []
    
    for file_info in processed_data:
        file_path = file_info['file_path']
        
        try:
            # Convert back to DataFrame
            df = pd.DataFrame(file_info['data'])
            
            # Transform data
            transformed_df = transform_data(df)
            
            # Load to database
            upsert_prices(transformed_df)
            
            # Mark as completed
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
    
    # Store final results
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
    invalid_files = context['task_instance'].xcom_pull(key='invalid_files') or []
    quality_failures = context['task_instance'].xcom_pull(key='quality_failures') or []
    failed_files = context['task_instance'].xcom_pull(key='failed_files') or []
    
    message = f"""
    ❌ ETL Pipeline Failed:
    
    Structure validation failures: {len(invalid_files)}
    Quality check failures: {len(quality_failures)}
    Transform/Load failures: {len(failed_files)}
    
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

validate_structure_task = BranchPythonOperator(
    task_id='validate_files',
    python_callable=validate_file_structure,
    dag=dag,
)

extract_quality_task = BranchPythonOperator(
    task_id='extract_data',
    python_callable=extract_and_quality_check,
    dag=dag,
)

transform_load_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_and_load_data,
    dag=dag,
)

# Success path
success_notification_task = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_success_notification,
    dag=dag,
)

# Failure handling tasks
handle_validation_failure_task = PythonOperator(
    task_id='handle_validation_failure',
    python_callable=send_failure_notification,
    dag=dag,
)

partial_validation_task = PythonOperator(
    task_id='partial_validation_success', 
    python_callable=lambda: print("Some files failed validation, continuing with valid files"),
    dag=dag,
)

handle_quality_failure_task = PythonOperator(
    task_id='handle_quality_failure',
    python_callable=send_failure_notification,
    dag=dag,
)

end_task = DummyOperator(
    task_id='end',
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag,
)

# Define task dependencies
start_task >> check_files_task

check_files_task >> [no_files_task, validate_structure_task]

validate_structure_task >> [
    handle_validation_failure_task,
    partial_validation_task,
    extract_quality_task
]

partial_validation_task >> extract_quality_task

extract_quality_task >> [handle_quality_failure_task, transform_load_task]

transform_load_task >> success_notification_task

[
    no_files_task,
    success_notification_task,
    handle_validation_failure_task,
    handle_quality_failure_task
] >> end_task
