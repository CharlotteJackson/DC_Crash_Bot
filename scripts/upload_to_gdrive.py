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

# Requirments
# sudo apt-get install libpq-dev python3-dev python3-pip
# pip install gspread pandas psycopg2 oauth2client

# Needed to connect to database
db_host = os.environ["DB_HOST"]
db_pass = os.environ["DB_PASS"]
db_user = os.environ["DB_USER"]
db_name = os.environ["DB_NAME"]

# Current query we are using
curr_query = "select reportdate, bicycle_fatalities as fatal_bicyclist, vehicle_fatalities as fatal_driver, pedestrian_fatalities as fatal_pedestrian, total_bicyclists as total_bicycles, total_pedestrians, drivers_impaired as driversimipared, drivers_speeding as speeding_involved, blockkey, 0 fatalpassenger, left(smd_id, 2) anc_id, smd_id, st_X(geography) x_coord, st_Y(geography) y_coord, intersectionid from analysis_data.dc_crashes_w_details where reportdate between date '2015-01-01 00:00:01' and current_date order by reportdate desc"

# Needed to upload to google drive
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive",
]


def upload_to_goole_sheets(df_loc: str) -> None:
    """
    Uploads csv to google drive.
    Args:
        - df_loc (str): A filepath to a csv file.
    Returns:
        N/A
    """
    # must have your custom creds
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        "gdrive_creds.json", scope
    )
    client = gspread.authorize(credentials)
    spreadsheet = client.open("dc_crash_bot")  # Name of spread sheet

    with open(df_loc, "r") as file_obj:
        content = file_obj.read()
        client.import_csv(spreadsheet.id, data=content)


def main():
    """
    Uploads data to google drive
    Args:
        N/A
    Returns:
        N/A
    """
    try:
        conn = psycopg2.connect(
            f"dbname='{db_name}' user='{db_user}' host='{db_host}' password='{db_pass}'"
        )
        logging.info("connected to db")
        df = psql.read_sql(curr_query, conn)

        # stop gdrive from converting
        df.update('"' + df["smd_id"].astype(str) + '"')
        # save csv
        df.to_csv("gdrive_test.csv", index=False)

        # upload csv to gdrive
        upload_to_goole_sheets("gdrive_test.csv")

    except Exception as error:
        logging.error(error)

    logging.info("done uploading")


if __name__ == "__main__":
    log_level = logging.INFO
    logging.basicConfig(
        format="%(asctime)s | %(levelname)s: %(message)s", level=log_level
    )
    main()