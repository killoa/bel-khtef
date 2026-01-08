import os
import json
import logging
from datetime import datetime, timedelta
import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.datasets import Dataset

# Target Directory Configuration
BASE_DIR = os.getenv("BELKHTEF_DATA_DIR", "/opt/airflow/data")
SILVER_DIR = os.path.join(BASE_DIR, "silver")
GOLD_DIR = os.path.join(BASE_DIR, "gold")

# Project Root for Website (Assuming dag is running from project root or simple relative path)
# In production, this would be an S3 bucket or valid static file host path
PROJECT_ROOT = os.getcwd() 

# Define Datasets
SILVER_DATASET = Dataset(f"file://{SILVER_DIR}/vehicles.parquet")
GOLD_DATASET = Dataset(f"file://{GOLD_DIR}/vehicles.parquet")

local_tz = pendulum.timezone("Africa/Tunis")

default_args = {
    'owner': 'Antigravity',
    'retries': 1,
}

@dag(
    dag_id='belkhtef_gold_publish',
    default_args=default_args,
    description='Gold Layer: Publish for Website',
    schedule=[SILVER_DATASET], # Triggered by Silver
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=['belkhtef', 'gold', 'publish'],
)
def gold_pipeline():

    @task(outlets=[GOLD_DATASET])
    def publish_gold(execution_date=None):
        """
        Reads Silver data, saves Gold Parquet, and generates website JSON.
        """
        silver_path = os.path.join(SILVER_DIR, "vehicles.parquet")
        
        if not os.path.exists(silver_path):
            raise Exception("Silver data not found.")
            
        df = pd.read_parquet(silver_path)
        
        # Gold Logic: Add metadata or specific formatting if needed
        # For now, just a pass-through plus metadata
        df['published_at'] = datetime.now().isoformat()

        # Filter out 0 or Null prices
        df = df[df['price_tnd'] > 0]
        
        # Save Gold Parquet
        os.makedirs(GOLD_DIR, exist_ok=True)
        gold_parquet_path = os.path.join(GOLD_DIR, "vehicles.parquet")
        df.to_parquet(gold_parquet_path, index=False)
        
        # Publish to Website JSON
        website_json_path = os.path.join(PROJECT_ROOT, "vehicles_gold.json")
        
        # Use to_json to handle numpy types
        df.to_json(website_json_path, orient='records', force_ascii=False, indent=4)
            
        logging.info(f"Published {len(df)} records to {website_json_path}")
        
        return website_json_path

    publish_gold()

gold_dag = gold_pipeline()
