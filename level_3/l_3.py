from helium import *
from selenium import *
import selenium 
from bs4 import BeautifulSoup
import asyncio
from google.oauth2 import service_account
from googleapiclient.discovery import build
import pandas as pd
import re
import spacy


# TODO: rendering the whole web page
def getting_data():
    url = 'https://www.olx.ua/uk/nedvizhimost/kvartiry/dolgosrochnaya-arenda-kvartir/kiev/?currency=UAH&search%5Bfilter_float_price:to%5D=15000&search%5Bfilter_enum_commission%5D%5B0%5D=1&search%5Bfilter_enum_pets%5D%5B0%5D=yes_cat'

    # Set up Selenium WebDriver
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')  # To run the browser in headless mode
    browser = webdriver.Chrome(options=options)
    browser.get(url)

    # Extracting data using Selenium
    titles = [element.text.strip() for element in browser.find_elements_by_css_selector('.css-16v5mdi.er34gjf0')]
    prices = [element.text.strip() for element in browser.find_elements_by_css_selector('.css-10b0gli.er34gjf0')]
    locations = [element.text.strip() for element in browser.find_elements_by_css_selector('.css-1a4brun.er34gjf0')]
    ploshcha = [element.text.strip() for element in browser.find_elements_by_css_selector('.css-643j0o')]

    # Constructing DataFrame
    all_data = {'All_data': titles, 'Price': prices, 'Location': locations, 'Area': ploshcha}
    df = pd.DataFrame(all_data)

    # Close the browser
    browser.quit()

    return df


# TODO: sending all the data to Google sheets (feed5)
async def update_sheet(df):
    # SCOPES = ['https:///www.googleapis.com/auth/sqkservice.admin']
    SERVICE_ACCOUNT_FILE = 'key.json'
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    # The ID and range of a sample spreadsheet.
    SAMPLE_SPREADSHEET_ID = "16QDDG-9-p-Ur1E_4UXoBvFh5eCrZgty9nkyGakycBF8"

    service_sheets = build("sheets", "v4", credentials=creds)

    # Convert DataFrame to a list of lists
    keys = df.columns.tolist()

    # Prepare the data for updating the sheet
    values = [keys]  # Append keys as the first row
    values.extend(df.values.tolist())  # Append DataFrame values

    body = {"values": values}

    try:
        # Update the spreadsheet with the new data
        result = service_sheets.spreadsheets().values().update(
            spreadsheetId=SAMPLE_SPREADSHEET_ID,
            range="feed5!A:F",
            valueInputOption="USER_ENTERED", body=body
        ).execute()
        print("Sheet updated successfully.")
        return result
    except Exception as e:
        print(f"Error updating sheet: {e}")
        return None


data = getting_data()
print(data)

asyncio.run(update_sheet(data))
