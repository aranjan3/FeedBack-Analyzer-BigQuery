from google.cloud import bigquery
import pandas as pd
import sys
import os
# Update path to the root of the project
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.config import BQ_PROJECT, BQ_DATASET, BQ_TABLE

def get_existing_timestamps():
    client = bigquery.Client(project=BQ_PROJECT)
    query = f"""
        SELECT DISTINCT Timestamp
        FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`
    """
    query_job = client.query(query)
    results = query_job.result()
    timestamps = [row["Timestamp"] for row in results]
    return pd.to_datetime(timestamps)