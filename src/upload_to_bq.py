import sys
import os
import pandas as pd
from google.cloud import bigquery
from bq_utils import get_existing_timestamps  # Ensure this function is correctly implemented

# Update path to the root of the project
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.config import BQ_PROJECT, BQ_DATASET, BQ_TABLE
from fetch_data import fetch_sheet_data
from sentiment_analysis import analyze_sentiment

def upload_to_bigquery(df):
    # Step 1: Convert Timestamp column to datetime
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
    print("📄 Converted Timestamp column to datetime.")
    
    # Step 2: Convert Google Sheet timestamps to UTC to match BigQuery
    df["Timestamp"] = df["Timestamp"].dt.tz_localize('UTC', ambiguous='NaT')
    print("🔍 Timestamp column after timezone conversion:")
    print(df["Timestamp"].head())

    # Step 3: Fetch existing timestamps from BigQuery
    existing_timestamps = get_existing_timestamps()  # This function should return a list or pandas series of timestamps in UTC
    print(f"🔍 Existing Timestamps in BigQuery: {existing_timestamps}")

    # Step 4: Filter out the rows that already exist in BigQuery
    df_new = df[~df["Timestamp"].isin(existing_timestamps)]
    
    # Step 5: Check if there is any new data to upload
    if df_new.empty:
        print("⚠️ No new data to upload. Skipping BigQuery upload.")
        return
    
    # Step 6: Upload new data to BigQuery
    client = bigquery.Client(project=BQ_PROJECT)
    table_ref = client.dataset(BQ_DATASET).table(BQ_TABLE)

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("Timestamp", "TIMESTAMP"),
            bigquery.SchemaField("Name", "STRING"),
            bigquery.SchemaField("Feedback", "STRING"),
            bigquery.SchemaField("sentiment", "FLOAT"),
            bigquery.SchemaField("sentiment_label", "STRING"),
        ],
        write_disposition="WRITE_APPEND",
    )

    load_job = client.load_table_from_dataframe(df_new, table_ref, job_config=job_config)
    load_job.result()  # Waits for the job to complete
    print("✅ Data uploaded to BigQuery successfully.")

def main():
    # Step 1: Fetch Google Sheet data
    df = fetch_sheet_data()
    print("📄 Columns in Sheet:", df.columns)

    # Step 2: Convert Timestamp to datetime
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
    print("🔍 Preview of DataFrame after Timestamp conversion:")
    print(df.head())

    # Step 3: Perform Sentiment Analysis
    df = analyze_sentiment(df)
    print("🔍 Preview of DataFrame with Sentiment:")
    print(df.head())

    # Step 4: Upload to BigQuery (Only new data)
    upload_to_bigquery(df)

if __name__ == "__main__":
    main()