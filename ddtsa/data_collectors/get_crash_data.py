# Python imports
import logging
import os
from typing import Tuple, Dict, Any, List

# 3rd Party Imports
# from datetime import datetime
from geopy.distance import geodesic
import pandas as pd

# postgress imports
import psycopg2
import pandas.io.sql as psql

# Project Imports
# TODO what is wrong with imports?
try:
    from .get_address import rev_geocode
except:
    from get_address import rev_geocode


# Check if google key found
try:
    GOOGLE_API_KEY = os.environ["GOOGLE_API_KEY"]
except Exception as error:
    logging.error("No GOOGLE API KEY detected")


# Needed to connect to database
db_host = os.environ["DB_HOST"]
db_pass = os.environ["DB_PASS"]
db_user = os.environ["DB_USER"]
db_name = os.environ["DB_NAME"]


def get_safety_concerns(address: str, gmap_data: Dict[str, Any] = None) -> str:
    """
    Purpose:
        Get safey concerns
    Args:
        address - current address
        gmap_data - google maps data
    Returns:
        text_string: formatted text
    """

    conn = psycopg2.connect(
        f"dbname='{db_name}' user='{db_user}' host='{db_host}' password='{db_pass}'"
    )
    logging.info("connected to db")

    # Get date form one year ago
    # year_ago_date_time = datetime.datetime.now() - datetime.timedelta(days=365)
    # date_format = "%Y-%m-%d %H:%M:%S"
    # year_ago_date_time_string = year_ago_date_time.strftime(date_format)

    if not gmap_data:
        gmap_data = rev_geocode(address, GOOGLE_API_KEY)

    # Get the latitude and longitude
    curr_lat = gmap_data[0]["geometry"]["location"]["lat"]
    curr_lng = gmap_data[0]["geometry"]["location"]["lng"]

    # Distance
    DIST = 321.868  # .2 miles

    # Query for crash data
    db_query = f"select mpdlatitude, mpdlongitude, reportdate, objectid, bicycle_injuries,vehicle_injuries, pedestrian_injuries,total_injuries,total_major_injuries,total_minor_injuries,bicycle_fatalities,pedestrian_fatalities, vehicle_fatalities from analysis_data.dc_crashes_w_details ad WHERE reportdate > date_trunc('month', CURRENT_DATE) - INTERVAL '1 year' order by reportdate desc"
    df = psql.read_sql(db_query, conn)

    # Query for moving violations
    moving_query = f"select issue_date,latitude, longitude,fine_amount, violation_code, violation_process_desc,plate_state, objectid FROM analysis_data.moving_violations mv WHERE issue_date > date_trunc('month', CURRENT_DATE) - INTERVAL '1 year' AND ST_DWithin(ST_MakePoint({curr_lng},{curr_lat}),mv.geography,{DIST}) order by issue_date desc"
    moving_df = psql.read_sql(moving_query, conn)

    # Query for waze data
    waze_query = f"select speed_limit,avg_speed_mph_moving_users FROM analysis_data.avg_waze_speed_by_roadway_block mv WHERE ST_DWithin(ST_MakePoint({curr_lng},{curr_lat}),mv.geography,{DIST})"
    waze_df = psql.read_sql(waze_query, conn)

    try:
        json_results = find_safety_concerns(df, address, gmap_data)
        moving_results = find_moving_concerns(moving_df, address, gmap_data)
        text_string = format_results(json_results, moving_results, waze_df)
    except Exception as error:
        json_results = {"error": error}
        text_string = error

    return text_string


def format_results(
    json_results: Dict[str, Any], moving_results: Dict[str, Any], waze_df
) -> str:
    """
    Purpose:
        Format the json to a text response
    Args:
        json_results: crash data
    Returns:
        text_string: formatted text
    """

    total_crashes = len(json_results)
    total_moving_violations = len(moving_results)

    text_string = f"There have been {total_crashes} crashes in the past year.  \n"
    text_string += f"There have been {total_moving_violations} moving violations in the past year.  \n"

    text_string += f"  \n"

    bicycle_injuries = 0
    vehicle_injuries = 0
    pedestrian_injuries = 0
    bicycle_fatalities = 0
    pedestrian_fatalities = 0
    vehicle_fatalities = 0

    for obj in json_results:

        bicycle_injuries += obj["bicycle_injuries"]
        vehicle_injuries += obj["vehicle_injuries"]
        pedestrian_injuries += obj["pedestrian_injuries"]
        bicycle_fatalities += obj["bicycle_fatalities"]
        pedestrian_fatalities += obj["pedestrian_fatalities"]
        vehicle_fatalities += obj["vehicle_fatalities"]

    text_string += f"Pedestrian Stats:  \n"
    text_string += f"pedestrian injuries: {pedestrian_injuries}   \n"
    text_string += f"pedestrian fatalities: {pedestrian_fatalities}  \n"
    text_string += f"  \n"

    text_string += f"Bicycle Stats:  \n"
    text_string += f"bicycle injuries: {bicycle_injuries}   \n"
    text_string += f"bicycle fatalities: {bicycle_fatalities}   \n"
    text_string += f"  \n"

    text_string += f"Vehicle Stats:  \n"
    text_string += f"vehicle injuries: {vehicle_injuries}   \n"
    text_string += f"vehicle fatalities: {vehicle_fatalities}   \n"
    text_string += f"  \n"

    type_json = {}
    plate_json = {}
    for obj in moving_results:

        mov_type = obj["violation_process_desc"]
        driver_plate = obj["plate_state"]

        if mov_type in type_json:
            type_json[mov_type] += 1
        else:
            type_json[mov_type] = 1

        if driver_plate in plate_json:
            plate_json[driver_plate] += 1
        else:
            plate_json[driver_plate] = 1

    mov_types = list(type_json.keys())
    if len(mov_types) > 0:
        text_string += "Moving violations types:  \n"
        for key in mov_types:
            text_string += f"{key} : {type_json[key]}  \n"
        text_string += f"  \n"

    plate_types = list(plate_json.keys())
    if len(plate_types) > 0:
        text_string += "State Plate types:  \n"
        for key in plate_types:
            text_string += f"{key} Plate Count: {plate_json[key]}  \n"

    text_string += f"  \n"
    # get waze speed
    waze_speed = waze_df["avg_speed_mph_moving_users"].mean()
    waze_speed_limit = waze_df["speed_limit"].max()

    text_string += f"Speed limit: {waze_speed_limit}  \n"
    text_string += f"Mean speed from Waze {waze_speed}  \n"

    count = 0
    for speed in list(waze_df["avg_speed_mph_moving_users"]):
        if speed > waze_speed_limit:
            count += 1

    count_len = len(list(waze_df["avg_speed_mph_moving_users"]))
    text_string += f"Counted {count} speeders out of {count_len} interactions ({round(float(count/count_len),2)}%) \n"

    return text_string


def fill_safety_json(
    row: pd.Series, crash_list: List[Dict[str, Any]], lat_long: Tuple
) -> None:
    """
    Purpose:
        Fill out unsafe times json
    Args:
        row: Row to check
        crash_json: has crash details
        lat_long: lat long of address to check
    Returns:
        N/A
    """

    try:
        row_lat_long = (row["mpdlatitude"], row["mpdlongitude"])

        # Did crash happen less than .2 miles from spot?
        if geodesic(row_lat_long, lat_long) < 0.2:

            crash_obj = {}
            crash_obj["id"] = row["objectid"]

            crash_obj["bicycle_injuries"] = row["bicycle_injuries"]
            crash_obj["vehicle_injuries"] = row["vehicle_injuries"]
            crash_obj["pedestrian_injuries"] = row["pedestrian_injuries"]
            crash_obj["bicycle_fatalities"] = row["bicycle_fatalities"]
            crash_obj["pedestrian_fatalities"] = row["pedestrian_fatalities"]
            crash_obj["vehicle_fatalities"] = row["vehicle_fatalities"]

            crash_list.append(crash_obj)
    except Exception as error:
        print(row)
        logging.error(error)
        return


def fill_moving_json(
    row: pd.Series, moving_list: List[Dict[str, Any]], lat_long: Tuple
) -> None:
    """
    Purpose:
        Fill out unsafe times json
    Args:
        row: Row to check
        crash_json: has crash details
        lat_long: lat long of address to check
    Returns:
        N/A
    """

    try:
        crash_obj = {}
        crash_obj["id"] = row["objectid"]
        crash_obj["fine_amount"] = row["fine_amount"]
        crash_obj["violation_code"] = row["violation_code"]
        crash_obj["violation_process_desc"] = row["violation_process_desc"]
        crash_obj["plate_state"] = row["plate_state"]

        moving_list.append(crash_obj)

    except Exception as error:
        print(row)
        logging.error(error)


def find_safety_concerns(
    df: pd.DataFrame, address: str, gmap_data: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Purpose:
        Find unsafe times given an address
    Args:
        df: Data to use
        address: address to use
    Returns:
        unsafe_times_json - > JSON with unsafe times
    """

    # geocode service - using google maps
    if not gmap_data:
        gmap_data = rev_geocode(address, GOOGLE_API_KEY)

    # Get the latitude and longitude
    curr_lat = gmap_data[0]["geometry"]["location"]["lat"]
    curr_lng = gmap_data[0]["geometry"]["location"]["lng"]

    lat_long = (curr_lat, curr_lng)

    # Can we leverage geopands?? #TODO

    crash_list = []

    df.apply(lambda row: fill_safety_json(row, crash_list, lat_long), axis=1)

    # print(crash_json)
    return crash_list


def find_moving_concerns(
    df: pd.DataFrame, address: str, gmap_data: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Purpose:
        Find unsafe times given an address
    Args:
        df: Data to use
        address: address to use
    Returns:
        unsafe_times_json - > JSON with unsafe times
    """

    # geocode service - using google maps
    if not gmap_data:
        gmap_data = rev_geocode(address, GOOGLE_API_KEY)

    # Get the latitude and longitude
    curr_lat = gmap_data[0]["geometry"]["location"]["lat"]
    curr_lng = gmap_data[0]["geometry"]["location"]["lng"]

    lat_long = (curr_lat, curr_lng)

    moving_list = []

    df.apply(lambda row: fill_moving_json(row, moving_list, lat_long), axis=1)

    # print(crash_json)
    return moving_list


def main():
    """
    Purpose:
        Test the function
    Args:
        N/A
    Returns:
        N/A
    """
    # sample address
    address = "R Street Northwest & 19th Street Northwest, Washington, DC"
    # address = "4915 Sheriff Rd NE, Washington, DC 20019"

    print(f"Testing using address: {address}")

    text_string = get_safety_concerns(address)

    print(text_string)


if __name__ == "__main__":
    main()
