from google.cloud import bigquery
import os
import sys
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
import tracemalloc
import aiohttp
import asyncio
tracemalloc.start()

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'key.json'

# TODO: конкурентно, кількома запитами до API, отримуєте вибіркові дані
#  (тобто в кожному запиті дані зі свого часового проміжку) з відкритого джерела Google Analytics у Bigquery
#  (датасет bigquery public-data:google_analytics_sample);
# Initialize a BigQuery client
client = bigquery.Client()

# Define your query
#   `bigquery-public-data.google_analytics_sample.ga_sessions_*`
query = """
SELECT
  *
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
LIMIT
  150
"""

# TODO: об'єднуєте ці дані в один датафрейм;
query_job = client.query(query)
df = query_job.to_dataframe()
print(df.keys())

# TODO: групуєте і агрегуєте дані в кілька інших датафреймів, кожен зі своїми групуваннями і агрегаціями;

df1_indexes = ['visitorId', 'visitNumber', 'visitId', 'visitStartTime']

# Select desired columns and set them as indexes
df1 = df[df1_indexes].copy()

# Fill missing values in the "visitorId" column with a specified value or method
# df1[df1['visitorId'] == ""].fillna({'Unknown'}, inplace=True)

# Convert data types if necessary
df1['visitorId'] = df1['visitorId'].astype(str)
df1['visitNumber'] = pd.to_numeric(df1['visitNumber'], errors='coerce').astype(int)
df1['visitId'] = pd.to_numeric(df1['visitId'], errors='coerce').astype(int)
df1['visitStartTime'] = pd.to_numeric(df1['visitStartTime'], errors='coerce').astype(int)  # Convert visitStartTime to int, handle errors

# Set indexes
# df1.set_index(['visitNumber', 'visitId', 'visitStartTime'], inplace=True)
# print(df1)


df2_indexes = ['date', 'totals', 'trafficSource', 'device', 'geoNetwork', 'customDimensions']
df2 = df[df2_indexes].copy()
print([type(i) for i in df2])


df2['date'] = df2['date'].astype(str)
df2['totals'] = df2['totals'].astype(str)
df2['trafficSource'] = df2['trafficSource'].astype(str)
df2['device'] = df2['device'].astype(str)
df2['geoNetwork'] = df2['geoNetwork'].astype(str)
df2['customDimensions'] = df2['customDimensions'].astype(str)

print(df2)

df3_indexes = ['hits', 'fullVisitorId', 'userId', 'clientId', 'channelGrouping', 'socialEngagementType']
df3 = df[df3_indexes].copy()
print([type(i) for i in df3])


df3['hits'] = df3['hits'].astype(str)
df3['fullVisitorId'] = df3['fullVisitorId'].astype(str)
df3['userId'] = df3['userId'].astype(str)
df3['clientId'] = df3['clientId'].astype(str)
df3['channelGrouping'] = df3['channelGrouping'].astype(str)
df3['socialEngagementType'] = df3['socialEngagementType'].astype(str)

print(df3)


# TODO: конкурентно записуєте всі ці датафрейми в Google Sheet, кожен датафрейм на свій аркуш.
async def update_sheet(df):
    # SCOPES = ['https:///www.googleapis.com/auth/sqkservice.admin']
    SERVICE_ACCOUNT_FILE = 'key.json'
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    # The ID and range of a sample spreadsheet.
    SAMPLE_SPREADSHEET_ID = "16QDDG-9-p-Ur1E_4UXoBvFh5eCrZgty9nkyGakycBF8"

    service_sheets = build("sheets", "v4", credentials=creds)

    # Replace pd.NA values with an empty string
    df = df.fillna(value="")

    # Extract keys from DataFrame
    keys = df.columns.tolist()

    # Prepare the data for updating the sheet
    values = [keys]  # Append keys as the first row
    values.extend(df.values.tolist())  # Append DataFrame values

    body = {"values": values}

    try:
        # Update the spreadsheet with the new data
        result = service_sheets.spreadsheets().values().update(
            spreadsheetId=SAMPLE_SPREADSHEET_ID,
            range="feed4!A:F",
            valueInputOption="USER_ENTERED", body=body
        ).execute()
        print("Sheet updated successfully.")
        return result
    except Exception as e:
        print(f"Error updating sheet: {e}")
        return None


asyncio.run(update_sheet(df3))
