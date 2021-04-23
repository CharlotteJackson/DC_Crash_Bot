# Usage
# python download_from_gdrive.py

# Follow this blog to get google creds
# https://medium.com/craftsmenltd/from-csv-to-google-sheet-using-python-ef097cb014f9


# Python imports
import os
import logging

# postgress imports
import pandas as pd
from sqlalchemy import create_engine

# gdrive imports
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.discovery import build


# aws imports
import boto3
from botocore.exceptions import ClientError

# Requirments
# sudo apt-get install libpq-dev python3-dev python3-pip
# pip install gspread pandas psycopg2 oauth2client boto3

# Needed to connect to database
db_host = os.environ["DB_HOST"]
db_pass = os.environ["DB_PASS"]
db_user = os.environ["DB_USER"]
db_name = os.environ["DB_NAME"]


# Needed to upload to google drive
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive",
]

# Ask Charlotte for creds
s3 = boto3.client("s3")


def upload_file_to_s3(file_name: str, bucket: str, object_name: str = None) -> bool:
    """
    Purpose:
        Uploads a file to s3
    Args/Requests:
         file_name: name of file
         bucket: s3 bucket name
         object_name: s3 name of object
    Return:
        Status: True if uploaded, False if failure
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    try:
        response = s3.upload_file(file_name, bucket, object_name)
        logging.info(response)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def download_file(file_id) -> None:
    """
    Downloas file from google drive
    Args:
        - file_id (str): file id to download
    Returns:
        N/A
    """
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


def clean_data() -> None:
    """
    Cleans the data we download
    Args:
        N/a
    Returns:
        N/A
    """
    df = pd.read_csv("dc_fss.csv")
    df.fillna("N/A")

    if "Unnamed: 22" in df:
        del df["Unnamed: 22"]

    # TODO clean up inputs, they have fields like "Adult" in the Age column
    # df["Age"] = pd.to_numeric(df["Age"])
    df.to_csv("dc_fss.csv", index=False)

    # TODO need write access to db
    # write_to_table(df)

    # Upload file to s3
    upload_file_to_s3(
        "dc_fss.csv", "dc-crash-bot-test", "source-data/dc-fss/dc_fss.csv"
    )


def write_to_table(df) -> None:
    """
    Writes data to database
    Args:
        N/a
    Returns:
        N/A
    """
    engine = create_engine(f"postgresql://{db_user}:{db_pass}@{db_host}:5432/{db_name}")
    df.to_sql("dc_fss", engine)


def main():
    """
    Downloads data to google drive
    Args:
        N/A
    Returns:
        N/A
    """
    # https://docs.google.com/spreadsheets/d/1uCDD5ox8t4WDNB377PomsS3PS_QInaoE4_lGmqtxb-M/edit#gid=0
    file_id = "1uCDD5ox8t4WDNB377PomsS3PS_QInaoE4_lGmqtxb-M"
    download_file(file_id)
    clean_data()
    logging.info("Done and done")


if __name__ == "__main__":
    log_level = logging.INFO
    logging.basicConfig(
        format="%(asctime)s | %(levelname)s: %(message)s", level=log_level
    )
    main()