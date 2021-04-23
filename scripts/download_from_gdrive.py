# Usage
# python upload_to_gdrive.py

# Follow this blog to get google creds
# https://medium.com/craftsmenltd/from-csv-to-google-sheet-using-python-ef097cb014f9


# Python imports
import os
import logging

# postgress imports
import psycopg2
import pandas.io.sql as psql

# gdrive imports
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from googleapiclient.discovery import build

# Requirments
# sudo apt-get install libpq-dev python3-dev python3-pip
# pip install gspread pandas psycopg2 oauth2client

# Needed to connect to database
# db_host = os.environ["DB_HOST"]
# db_pass = os.environ["DB_PASS"]
# db_user = os.environ["DB_USER"]
# db_name = os.environ["DB_NAME"]



# Needed to upload to google drive
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive",
]


def mimetype_save(mimetype):

    if mimetype == "application/vnd.google-apps.document":
        return "text/plain", ".txt"
    elif mimetype == "application/vnd.google-apps.spreadsheet":
        return "text/csv", ".csv"
    # Todo ADD other types
    else:
        logging.warn(f"skipping mimetype {mimetype}")
        return None, None


def download_file(file_id):

    # must have your custom creds
    creds = ServiceAccountCredentials.from_json_keyfile_name("gdrive_creds.json", scope)

    service = build("drive", "v3", credentials=creds)

    request = service.files().export_media(fileId=file_id, mimeType="text/csv")

    print(request)
    fh = open(f"dc_fss.csv", "wb")
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print("Download %d%%." % int(status.progress() * 100))


def main():
    """
    Uploads data to google drive
    Args:
        N/A
    Returns:
        N/A
    """
    # https://docs.google.com/spreadsheets/d/1uCDD5ox8t4WDNB377PomsS3PS_QInaoE4_lGmqtxb-M/edit#gid=0
    file_id = "1uCDD5ox8t4WDNB377PomsS3PS_QInaoE4_lGmqtxb-M"
    download_file(file_id)


if __name__ == "__main__":
    log_level = logging.INFO
    logging.basicConfig(
        format="%(asctime)s | %(levelname)s: %(message)s", level=log_level
    )
    main()