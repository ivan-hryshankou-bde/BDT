import pandas as pd
import re
import string
import logging
import os
from datetime import datetime

from airflow.sdk import dag, task, task_group, Asset
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mongo.hooks.mongo import MongoHook

# Configure of logging
LOG_DIR = "/opt/airflow/logs/custom-logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "etl_gcs_mongo_report.log")

logger = logging.getLogger("airflow.task")

fh = logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8')
fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(fh)

# Connections in Airflow
GCP_CONN_ID = 'GCP_conn'
MINIO_CONN_ID = 'Minio_conn'
MONGO_CONN_ID = 'Mongo_conn'

# GCS source config
GCS_BUCKET = "airflow-project-bucket-gcp-to-mongo" 
INPUT_FILE = "tiktok_google_play_reviews.csv"
GCS_PATH = f"gs://{GCS_BUCKET}/{INPUT_FILE}"

# Minio staging config
MINIO_BUCKET = "airflow-project-minio-stage-bucket"

# Minio path helper
def get_s3_path(filename):
    """Generates an S3-compatible URI for MinIO storage"""
    return f"s3://{MINIO_BUCKET}/{filename}"

# MinIO stage paths
FILE_STEP_1 = 'temp/step1_nulls_replaced.csv'
FILE_STEP_2 = 'temp/step2_sorted.csv'
FINAL_FILE = 'final/processed_data.csv'

# Asset for final file in MinIO
processed_asset = Asset(uri=f"s3://{MINIO_BUCKET}/{FINAL_FILE}", name="processed_reviews_csv")

# ---------------------------------------------------------
# DAG 1: Processor (GCS Source to MinIO Staging)
# ---------------------------------------------------------
@dag(
    dag_id='1_etl_processor_gcp_to_minio',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=['etl', 'gcs_source', 'minio_staging']
)
def processor_dag():
    """
    Orchestrates the ETL pipeline by validating GCS source, 
    performing multi-stage cleaning in MinIO, and updating the Data Asset
    """

    # Sensor for waiting the file in Google Cloud
    wait_for_file = GCSObjectExistenceSensor(
        task_id='wait_for_file_gcs',
        bucket=GCS_BUCKET,
        object=INPUT_FILE,
        google_cloud_conn_id=GCP_CONN_ID,
        poke_interval=10,
        timeout=600
    )

    @task.branch(task_id='check_file_empty')
    def check_file_empty():
        """Checks if the GCS blob exists and contains data"""

        hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
        blob = hook.get_conn().bucket(GCS_BUCKET).get_blob(INPUT_FILE)

        if blob and blob.size > 0:
            logger.info(f"Source file {INPUT_FILE} found. Size: {blob.size} bytes.")
            return "processing_group.step1_gcs_to_minio_replace_nulls"
        
        return "log_empty"

    @task(task_id='log_empty')
    def log_empty_task():
        """Logs an error due the file is empty"""
        logger.error(f"File {INPUT_FILE} in GCS has no content. Pipeline halted.")

    @task_group(group_id='processing_group')
    def processing_tasks():
        """Contains core transformation logic using MinIO as a staging area"""

        @task(task_id='step1_gcs_to_minio_replace_nulls')
        def task_step1():
            """Ingestion: Replaces NULL values with placeholders"""
            logger.info("Step 1: Ingesting GCS data and filling NULL values.")
            df = pd.read_csv(GCS_PATH)
            
            df.fillna("-", inplace=True)

            s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
            if not s3_hook.check_for_bucket(MINIO_BUCKET):
                logger.warning(f"Staging minio bucket {MINIO_BUCKET} does not exist. Creating a new one.")
                s3_hook.create_bucket(MINIO_BUCKET)

            path_out = get_s3_path(FILE_STEP_1)
            df.to_csv(path_out, index=False)
            logger.info(f"Step 1 completed. Ingested file saved: {path_out}")

        @task(task_id='step2_sort_full_file')
        def task_step2():
            """Normalization: Converts timestamps and sorts data chronologically"""
            logger.info("Step 2: Sorting dataset by timestamp.")
            path_in = get_s3_path(FILE_STEP_1)
            df = pd.read_csv(path_in)
            
            if 'at' in df.columns:
                df['at'] = pd.to_datetime(df['at'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
                df.sort_values(by='at', inplace=True)
            
            path_out = get_s3_path(FILE_STEP_2)
            df.to_csv(path_out, index=False)
            logger.info(f"Step 2 completed. Sorted file saved: {path_out}")

        @task(task_id='step3_clean_content', outlets=[processed_asset])
        def task_step3():
            """Sanitization: Removes non-alphanumeric noise and handles blank strings"""
            logger.info("Step 3: Sanitizing review content and stripping whitespaces.")
            path_in = get_s3_path(FILE_STEP_2)
            df = pd.read_csv(path_in)

            allowed_punc = re.escape(string.punctuation)
            pattern = rf"[^\w\s{allowed_punc}]"

            if 'content' in df.columns:
                df['content'] = df['content'].str.replace(pattern, '', regex=True)
                df['content'] = df['content'].str.strip()
                df.loc[df['content'] == "", 'content'] = "-"
            
            path_out = get_s3_path(FINAL_FILE)
            df.to_csv(path_out, index=False)
            logger.info(f"Step 3 completed. Sanitized final processed file ready: {path_out}")

        # The order of group execution
        t1 = task_step1()
        t2 = task_step2()
        t3 = task_step3()
        
        t1 >> t2 >> t3

    # The order of the main execution
    check = check_file_empty()
    gw = processing_tasks()
    log = log_empty_task()

    wait_for_file >> check
    check >> log
    check >> gw

processor_dag_instance = processor_dag()

# ---------------------------------------------------------
# DAG 2: Loader (MinIO Staging to MongoDB Target)
# ---------------------------------------------------------
@dag(
    dag_id='2_etl_loader_minio_to_mongo',
    start_date=datetime(2026, 1, 1),
    schedule=[processed_asset],
    catchup=False,
    tags=['etl', 'minio_staging', 'mongo_target']
)
def loader_dag():
    """
    Consumes processed CSV from MinIO to MongoDB collection 
    being triggered by Asset update
    """

    @task(task_id='load_minio_final_to_mongo')
    def load_to_mongo():
        """Loads final dataset into MongoDB, ensuring schema consistency and idempotency"""
        logger.info("Initiating data load from MinIO to MongoDB.")
        
        path = get_s3_path(FINAL_FILE)
        df = pd.read_csv(path)
        
        if 'at' in df.columns:
                df['at'] = pd.to_datetime(df['at'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

        mongo_hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
        client = mongo_hook.get_conn()
        db = client.analytics_db 
        collection = db.reviews
        
        collection.delete_many({})
        logger.info("Target MongoDB collection cleared for fresh ingestion.")

        collection.insert_many(df.to_dict(orient='records'))
        logger.info(f"Success: Inserted {len(df)} documents into MongoDB (DB: analytics_db, Coll: reviews)")

    load_to_mongo()

loader_dag_instance = loader_dag()