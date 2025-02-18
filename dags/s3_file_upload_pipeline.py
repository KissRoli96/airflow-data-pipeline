import boto3
import os
import logging
from airflow.operators.python import PythonOperator

# AWS S3 Bucket Details
S3_BUCKET = "airflow-data-bucket-1" 
DATA_DIR = "/opt/airflow/data"  # Directory for processed data files
SOURCE_DIR = "/opt/airflow/dags"  # Directory for project source files

# Define allowed file types for data and source files
ALLOWED_DATA_TYPES = [".csv", ".txt", ".json"]
ALLOWED_SOURCE_TYPES = [".py", ".yaml", ".md"]

def upload_files_to_s3():
    try:
        logging.info("Scanning directories for files to upload...")

        s3 = boto3.client("s3")

        # 🔹 Upload Data Files
        for file in os.listdir(DATA_DIR):
            file_path = os.path.join(DATA_DIR, file)

            if any(file.endswith(ext) for ext in ALLOWED_DATA_TYPES):
                s3_key = f"data_files/{file}"
                s3.upload_file(file_path, S3_BUCKET, s3_key)
                logging.info(f"Uploaded data file: {file} → s3://{S3_BUCKET}/{s3_key}")

        # 🔹 Upload Source Files
        for file in os.listdir(SOURCE_DIR):
            file_path = os.path.join(SOURCE_DIR, file)

            if any(file.endswith(ext) for ext in ALLOWED_SOURCE_TYPES):
                s3_key = f"source_files/{file}"
                s3.upload_file(file_path, S3_BUCKET, s3_key)
                logging.info(f"Uploaded source file: {file} → s3://{S3_BUCKET}/{s3_key}")

        logging.info("File upload process completed.")

    except Exception as e:
        logging.error(f"Error uploading files to S3: {str(e)}")
        raise

# Define the Airflow DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "s3_file_upload_pipeline",
    default_args=default_args,
    schedule_interval="0 12 * * *",  # Runs every day at 12:00 UTC
    catchup=False,
)

task_upload_all_files_s3 = PythonOperator(
    task_id="upload_all_files_s3",
    python_callable=upload_files_to_s3,
    dag=dag,
)

# Update task dependencies
task_upload_all_files_s3
