import sys
import os

# Add the 'src' directory to the system path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

# Use the GOOGLE_APPLICATION_CREDENTIALS environment variable to fetch credentials
google_creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", None)
if not google_creds:
    print("❌ GOOGLE_APPLICATION_CREDENTIALS environment variable not set.")
    exit(1)

# Now, you can import the modules correctly
from fetch_data import fetch_sheet_data
from sentiment_analysis import analyze_sentiment
from upload_to_bq import upload_to_bigquery

def main():
    # Fetch data
    df = fetch_sheet_data()
    
    # Analyze sentiment
    df = analyze_sentiment(df)
    
    # Upload to BigQuery
    upload_to_bigquery(df)

if __name__ == "__main__":
    main()
