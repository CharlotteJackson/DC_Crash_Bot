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
# pip install gspread pandas psycopg2 oauth2client


db_host = os.environ["DB_HOST"]
db_pass = os.environ["DB_PASS"]
db_user = os.environ["DB_USER"]
db_name = os.environ["DB_NAME"]


curr_query = "select reportdate, bicycle_fatalities as fatal_bicyclist, vehicle_fatalities as fatal_driver, pedestrian_fatalities as fatal_pedestrian, total_bicyclists as total_bicycles, total_pedestrians, drivers_impaired as driversimipared, drivers_speeding as speeding_involved, blockkey, 0 fatalpassenger, left(smd_id, 2) anc_id, smd_id, st_X(geography) x_coord, st_Y(geography) y_coord, intersectionid from analysis_data.dc_crashes_w_details where reportdate between date '2015-01-01 00:00:01' and current_date order by reportdate desc"


scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive",
]


def upload_to_goole_sheets(df_loc):

    # must have your custom creds
    # Good write up https://medium.com/craftsmenltd/from-csv-to-google-sheet-using-python-ef097cb014f9
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        "gdrive_creds.json", scope
    )
    client = gspread.authorize(credentials)
    spreadsheet = client.open("dc_crash_bot")  # Name of spread sheet

    with open(df_loc, "r") as file_obj:
        content = file_obj.read()
        client.import_csv(spreadsheet.id, data=content)


def main():

    try:
        conn = psycopg2.connect(
            f"dbname='{db_name}' user='{db_user}' host='{db_host}' password='{db_pass}'"
        )
        logging.info("connected to db")
        df = psql.read_sql(curr_query, conn)
        # save csv
        df.to_csv("gdrive_test.csv")

        # upload csv to gdrive
        upload_to_goole_sheets("gdrive_test.csv")

    except:
        logging.error("failed to connect to db")


if __name__ == "__main__":
    main()