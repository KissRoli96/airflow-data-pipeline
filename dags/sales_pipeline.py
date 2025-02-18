import logging
import os
import pandas as pd
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# PostgreSQL connection settings
DB_SETTINGS = {
    "host": "postgres",
    "database": "airflow_db",
    "user": "airflow",
    "password": "airflow"
}

DATA_DIR = "/opt/airflow/data"

# Ensure directory exists
os.makedirs(DATA_DIR, exist_ok=True)

# Task 1: Read and process CSV file
def process_csv():
    try:
        logging.info("Reading CSV file...")
        df = pd.read_csv(f"{DATA_DIR}/sales_data.csv")

        # Data transformation
        df['date'] = pd.to_datetime(df['date'], errors='coerce')  # Convert date format
        df.rename(columns={'sales_amount': 'amount'}, inplace=True)  # Rename column

        # Check if required columns exist
        if not all(col in df.columns for col in ['id', 'date', 'amount', 'currency']):
            raise ValueError("Missing required columns in CSV file")

        # Save transformed data
        df.to_csv(f"{DATA_DIR}/sales_data_transformed.csv", index=False)
        logging.info("CSV processing completed successfully!")

    except Exception as e:
        logging.error(f"Error processing CSV: {str(e)}")
        raise  # Rethrow exception to Airflow

# Task 2: Load transformed data into PostgreSQL
def load_to_postgres():
    try:
        logging.info("Loading data into PostgreSQL...")
        df = pd.read_csv(f"{DATA_DIR}/sales_data_transformed.csv")

        conn = psycopg2.connect(**DB_SETTINGS)
        cur = conn.cursor()

        # Clear old data before inserting
        cur.execute("DELETE FROM sales;")

        # Insert data
        for _, row in df.iterrows():
            cur.execute(
            """
            INSERT INTO sales (date, amount, currency) 
            VALUES (%s, %s, %s)
            ON CONFLICT (date, amount, currency) DO NOTHING;
            """,
            (row["date"], row["amount"], row["currency"]),
        )



        conn.commit()
        cur.close()
        conn.close()
        logging.info("Data successfully loaded into PostgreSQL!")

    except Exception as e:
        logging.error(f"Error loading data to PostgreSQL: {str(e)}")
        raise  # Rethrow exception to Airflow

# Airflow DAG Definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sales_pipeline',
    default_args=default_args,
    schedule_interval="0 6 * * *", # RUNS every day at 06:00 UTC
    catchup=False
)

# Define tasks
task_1 = PythonOperator(
    task_id='process_csv',
    python_callable=process_csv,
    dag=dag,
)

task_2 = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag,
)

# Task dependencies
task_1 >> task_2
