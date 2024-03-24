import PIL.Image
from googleapiclient.discovery import build
from google.oauth2 import service_account
import requests
from PIL import Image
import io
import aiohttp
import asyncio
from aiohttp import ClientSession
import tracemalloc
tracemalloc.start()


def data_1():

    # SCOPES = ['https:///www.googleapis.com/auth/sqkservice.admin']
    SERVICE_ACCOUNT_FILE = 'key.json'
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    # The ID and range of a sample spreadsheet.
    SAMPLE_SPREADSHEET_ID = "16QDDG-9-p-Ur1E_4UXoBvFh5eCrZgty9nkyGakycBF8"

    service = build("sheets", "v4", credentials=creds)
    sheet = service.spreadsheets()

    result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range="feed!A:B").execute()
    # print(result)

    values = result.get("values", [])
    return values


async def process_image(url):
    try:
        async with aiohttp.ClientSession(trust_env=True) as session:
            async with session.get(url, ssl=False) as response:
                content = await response.read()
                image_file = io.BytesIO(content)

                # Open the image using PIL
                image = Image.open(image_file)

                # Process the image as needed
                width, height = image.size
                final_answer = f'{width}x{height}'
                return str(final_answer)
    except Exception as e:
        return f"Error processing image from {url}: {e}"


# async def main():
#     all_data = data_1()
#     image_sizes = []
#
#     async with aiohttp.ClientSession(trust_env=True) as session:
#         for data_row in all_data[1:]:  # Skip header row
#             url = data_row[0]  # Assuming the URL is in the first column of each row
#             size = await process_image(url)
#             image_sizes.append(size)
#
#     return image_sizes


async def main():
    all_data = data_1()
    url_and_sizes = []

    async with aiohttp.ClientSession(trust_env=True) as session:
        for data_row in all_data[1:15]:  # Skip header row
            url = data_row[0]  # Assuming the URL is in the first column of each row
            size = await process_image(url)
            url_and_sizes.append([url, size])  # Collect both URL and image size

    return url_and_sizes


async def update_sheet(url_and_sizes):
    SERVICE_ACCOUNT_FILE = 'key.json'
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    SAMPLE_SPREADSHEET_ID = "16QDDG-9-p-Ur1E_4UXoBvFh5eCrZgty9nkyGakycBF8"

    service = build("sheets", "v4", credentials=creds)
    sheet = service.spreadsheets()

    # Prepare the data for updating the sheet
    values = [["", size] for _, size in url_and_sizes]
    body = {"values": values}

    # Update the spreadsheet with the new data
    result = sheet.spreadsheets().values().update(
        spreadsheetId=SAMPLE_SPREADSHEET_ID,
        range="feed2!A:B",
        valueInputOption="USER_ENTERED", body=body
    ).execute()
    print("Sheet updated successfully.")
    return result


if __name__ == "__main__":

    all_data = data_1()
    url = data_1()[:][1][:]

    # NOT NEEDED SINCE I'M NOT USING WINDOWS
    # policy = asyncio.WindowsSelectorEventLoopPolicy()
    # asyncio.set_event_loop_policy(policy)
    image_sizes = asyncio.run(main())
    print(image_sizes)
    print(update_sheet(image_sizes))
