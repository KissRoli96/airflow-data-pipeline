import logging
import os
import pandas as pd
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

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

# Task 1: Read and process CSV file using Apache Spark
def process_csv():
    try:
        logging.info("Initializing Spark Session...")
        
        # Initialize a Spark session
        spark = SparkSession.builder.appName("SalesProcessing").getOrCreate()

        logging.info("Reading CSV file using Apache Spark...")

        # Read the CSV file into a Spark DataFrame
        df = spark.read.option("header", True).option("inferSchema", True).csv(f"{DATA_DIR}/sales_data.csv")

        logging.info("Processing data...")

        # Rename column "sales_amount" to "amount"
        df = df.withColumnRenamed("sales_amount", "amount")

        # Convert all column names to lowercase
        df = df.select([col(column).alias(column.lower()) for column in df.columns])

        # Drop any rows with NULL values
        df = df.dropna()

        logging.info("Data processing completed. Saving transformed CSV...")

        # Save the transformed data back as a CSV file (single file)
        output_path = f"{DATA_DIR}/sales_data_transformed.csv"
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{DATA_DIR}/sales_data_transformed_tmp")

        # Rename the Spark output file to ensure Pandas can read it
        spark_output_dir = f"{DATA_DIR}/sales_data_transformed_tmp"
        files = os.listdir(spark_output_dir)
        for file in files:
            if file.endswith(".csv"):
                os.rename(f"{spark_output_dir}/{file}", output_path)
                break

        # Remove the temporary Spark folder
        os.rmdir(spark_output_dir)

        logging.info("File saved successfully!")

        # Stop Spark session
        spark.stop()

    except Exception as e:
        logging.error(f"Error processing CSV with Spark: {str(e)}")
        raise

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
        raise

# Airflow DAG Definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "sales_pipeline",
    default_args=default_args,
    schedule_interval="0 6 * * *",  # Runs every day at 06:00 UTC
    catchup=False,
)

# Define tasks
task_1 = PythonOperator(
    task_id="process_spark_csv", 
    python_callable=process_csv,
    dag=dag,
)

task_2 = PythonOperator(
    task_id="load_to_postgres",
    python_callable=load_to_postgres,
    dag=dag,
)

# Task dependencies
task_1 >> task_2
