import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

def fetch_sheet_data():
    # Google Sheets API authentication
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(r'C:\APOORV\PROJECTS\Customer_Sentiment_Analysis\creds\creds.json', scope)
    client = gspread.authorize(creds)

    # Open the Google Sheet by name and fetch data from first sheet
    sheet = client.open("Customer Feedback (Responses)").sheet1
    data = sheet.get_all_records()

    # Convert the fetched data into a pandas DataFrame
    df = pd.DataFrame(data)
    return df

# Uncomment below to test the function
# df = fetch_sheet_data()
# print(df.head())