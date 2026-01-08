import os
import json
import logging
from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag, task
from airflow.datasets import Dataset

# Target Directory Configuration
BASE_DIR = os.getenv("BELKHTEF_DATA_DIR", "/opt/airflow/data")
BRONZE_DIR = os.path.join(BASE_DIR, "bronze")

# Define Datasets
BRONZE_DATASET = Dataset(f"file://{BRONZE_DIR}/raw_vehicles.json")

# Local timezone for Tunis
local_tz = pendulum.timezone("Africa/Tunis")

default_args = {
    'owner': 'Antigravity',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=60),
}

@dag(
    dag_id='belkhtef_bronze_scraper',
    default_args=default_args,
    description='Bronze Layer: Scrape Tayara vehicle data',
    schedule_interval='0 9 * * 1', # Monday at 9 AM
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=['belkhtef', 'bronze', 'scraping'],
)
def bronze_pipeline():

    @task(outlets=[BRONZE_DATASET])
    def extract_bronze(execution_date=None):
        """
        Scrapes raw data from Tayara.tn and saves it as a partitioned JSON.
        """
        # Note: In production, ensure scraper is in PYTHONPATH
        from scraper import scrape_tayara_vehicles
        
        logging.info("Starting Tayara.tn scrape...")
        # Production scrape size or parameterized
        raw_data = scrape_tayara_vehicles(pages=5) 
        
        # We save to a specific 'latest' file for the Dataset trigger to be consistent
        # In a real lake architecture, we might append or use key-based partitions
        output_path = os.path.join(BRONZE_DIR, "raw_vehicles.json")
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, ensure_ascii=False, indent=4)
            
        logging.info(f"Saved {len(raw_data)} raw records to {output_path}")
        return output_path

    extract_bronze()

bronze_dag = bronze_pipeline()
