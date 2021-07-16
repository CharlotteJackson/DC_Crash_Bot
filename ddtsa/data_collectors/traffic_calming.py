# Python imports
import logging
import os
from typing import Dict, Any, Tuple, List
from geopy.distance import geodesic

# 3rd Party Imports
import pandas as pd
import psycopg2
import pandas.io.sql as psql


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


def get_traffic_calming(address: str, gmap_data: Dict[str, Any] = None) -> str:
    """
    Purpose:
        Get traffic calming near address
    Args:
        address: current address
        gmap_data: google maps data
    Returns:
        text_string: formatted text with traffic calming info
    """
    # TODO
    json_results = {}

    # Connect to postgress db
    conn = psycopg2.connect(
        f"dbname='{db_name}' user='{db_user}' host='{db_host}' password='{db_pass}'"
    )
    logging.info("connected to db")

    # geocode service - using google maps
    if not gmap_data:
        gmap_data = rev_geocode(address, GOOGLE_API_KEY)

    # Get the latitude and longitude
    curr_lat = gmap_data[0]["geometry"]["location"]["lat"]
    curr_lng = gmap_data[0]["geometry"]["location"]["lng"]

    # Distance
    DIST = 321.868  # .2 miles

    db_query = f"select blockkey,nbh_cluster_names,speed_limit,num_sides_w_sidewalks,totalbikelanes,totalraisedbuffers,totalparkinglanes from analysis_data.roadway_blocks ad WHERE ST_DWithin(ST_MakePoint({curr_lng},{curr_lat}),ad.geography,{DIST})"

    df = psql.read_sql(db_query, conn)


    try:
        json_results = find_traffic_calming_features(df, gmap_data)

        # check speed cameras
        speed_cameras = find_nearby_speed_cameras(gmap_data)

        # check speed humps
        speed_humps = find_nearby_speed_humps(gmap_data)

        text_string = format_traffic_calming_json(
            json_results, speed_cameras, speed_humps
        )
    except Exception as error:
        json_results = {"error": error}
        text_string = error

    return text_string


def find_nearby_speed_humps(gmap_data: Dict[str, Any]):
    """
    Purpose:
        Find nearby speed cameras
    Args:
        gmap_data: google maps data
    Returns:
        speed_camera data: list of speed cameras
    """
    # Get the latitude and longitude
    curr_lat = gmap_data[0]["geometry"]["location"]["lat"]
    curr_lng = gmap_data[0]["geometry"]["location"]["lng"]

    # Create tuple of lat long
    lat_long = (curr_lat, curr_lng)

    # speed camera dataframe
    df = pd.read_csv("aux_data/full_speed_humps.csv")

    speed_cameras = []

    # Find speed humps within .2 miles
    df.apply(lambda row: fill_speed_humps(row, speed_cameras, lat_long), axis=1)

    return speed_cameras


def find_nearby_speed_cameras(gmap_data: Dict[str, Any]):
    """
    Purpose:
        Find nearby speed cameras
    Args:
        gmap_data: google maps data
    Returns:
        speed_camera data: list of speed cameras
    """
    # Get the latitude and longitude
    curr_lat = gmap_data[0]["geometry"]["location"]["lat"]
    curr_lng = gmap_data[0]["geometry"]["location"]["lng"]

    # Create tuple of lat long
    lat_long = (curr_lat, curr_lng)

    # speed camera dataframe
    df = pd.read_csv("aux_data/DC_Automated_Cameras_2018_to_2020.csv")

    speed_cameras = []

    # Find speed cameras within .2 miles
    df.apply(lambda row: fill_speed_cameras(row, speed_cameras, lat_long), axis=1)

    return speed_cameras


def format_traffic_calming_json(
    traffic_calming_list: List[Dict[str, Any]],
    speed_cameras: List[Dict[str, Any]],
    speed_humps: List[Dict[str, Any]],
) -> str:
    """
    Purpose:
        Format the json to a text response
    Args:
        traffic_calming_json: traffic calmaing data
    Returns:
        text: formatted text
    """
    text = ""

    if len(speed_cameras) > 0:

        for camera in speed_cameras:
            text += (
                f'There is a {camera["camera_type"]} camera at {camera["address"]}  \n'
            )

        text += "\n"
    else:
        text += "No speed cameras: \n\n"

    if len(speed_humps) > 0:

        for hump in speed_humps:

            if int(hump["count"]) > 1:
                text += (
                    f'There are {hump["count"]} speed humps at {hump["location"]}  \n'
                )
            else:
                text += f'There is {hump["count"]} speed hump at {hump["location"]}  \n'

        text += "\n"
    else:
        text += "No speed humps: \n\n"

    text += "Other Traffic Calming Measures: \n\n"

    # Maybe just get the max

    totalbikelanes_max = 0
    totalraisedbuffers_max = 0
    totalparkinglanes_max = 0
    speedlimit_max = 0

    for traffic_calming_json in traffic_calming_list:

        if traffic_calming_json["totalbikelanes"] > totalbikelanes_max:
            totalbikelanes_max = traffic_calming_json["totalbikelanes"]

        if traffic_calming_json["totalraisedbuffers"] > totalraisedbuffers_max:
            totalraisedbuffers_max = traffic_calming_json["totalraisedbuffers"]

        if traffic_calming_json["totalparkinglanes"] > totalparkinglanes_max:
            totalparkinglanes_max = traffic_calming_json["totalparkinglanes"]

        if traffic_calming_json["speedlimit"] > speedlimit_max:
            speedlimit_max = traffic_calming_json["speedlimit"]

    # text += f'{traffic_calming_json["nbh_cluster_names"]} - {traffic_calming_json["blockkey"]}   \n'
    text += f"There are {totalbikelanes_max} bike lanes  \n"
    text += f"There are {totalraisedbuffers_max} raised buffers  \n"
    text += f"There are {totalparkinglanes_max} parking lanes  \n"
    text += f"The speed limit is {speedlimit_max} MPH  \n"
    text += f"\n\n"

    return text


def fill_speed_humps(
    row: pd.Series, speed_humps_list: Dict[str, Any], lat_long: Tuple
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
        row_lat_long = (row["lat"], row["lng"])
        # print(row_lat_long)
    except Exception as error:
        print(row)
        logging.error(error)
        return

    # Did crash happen less than .2 miles from spot?
    if geodesic(row_lat_long, lat_long) < 0.2:

        speed_humps_json = {}
        speed_humps_json["location"] = row["location"]
        speed_humps_json["count"] = row["count"]
        speed_humps_list.append(speed_humps_json)


def fill_speed_cameras(
    row: pd.Series, speed_cameras_list: Dict[str, Any], lat_long: Tuple
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
        row_lat_long = (row["Y"], row["X"])
        # print(row_lat_long)
    except Exception as error:
        print(row)
        logging.error(error)
        return

    # Did crash happen less than .2 miles from spot?
    if geodesic(row_lat_long, lat_long) < 0.2:

        speed_camera_json = {}

        speed_camera_json["speed"] = row["SPEED"]
        speed_camera_json["address"] = row["ADDRESS"]
        speed_camera_json["camera_type"] = row["CAMERA_TYPE"]

        speed_cameras_list.append(speed_camera_json)


def fill_calming_json(
    row: pd.Series, traffic_calming_list: Dict[str, Any], lat_long: Tuple
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

        traffic_calming_json = {}
        traffic_calming_json["blockkey"] = row["blockkey"]
        traffic_calming_json["nbh_cluster_names"] = row["nbh_cluster_names"]
        traffic_calming_json["speedlimit"] = int(row["speed_limit"])
        traffic_calming_json["num_sides_w_sidewalks"] = int(
            row["num_sides_w_sidewalks"]
        )
        traffic_calming_json["totalbikelanes"] = int(row["totalbikelanes"])
        traffic_calming_json["totalraisedbuffers"] = int(row["totalraisedbuffers"])
        traffic_calming_json["totalparkinglanes"] = int(row["totalparkinglanes"])
        traffic_calming_list.append(traffic_calming_json)

    except Exception as error:
        print(row)
        logging.error(error)
        return


def find_traffic_calming_features(
    df: pd.DataFrame, gmap_data: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Purpose:
        Find traffic calming features given an address
    Args:
        df: Dataframe of our data
        address: adress to find features for
    Returns:
        traffic_calming_json: JSON of traffic calming data
    """

    # hmm road_way_blocks dont have latlong this will be annoying to convert everything..

    # we would use the geodesic function to see if latlong withint .2 miles
    # for now we will hard code to row 0

    # speedlimit
    traffic_calming_list = []

    # Get the latitude and longitude
    curr_lat = gmap_data[0]["geometry"]["location"]["lat"]
    curr_lng = gmap_data[0]["geometry"]["location"]["lng"]

    # Create tuple of lat long
    lat_long = (curr_lat, curr_lng)
    # print(lat_long)

    # Find all roadway blocks within .1 miles of address
    df.apply(lambda row: fill_calming_json(row, traffic_calming_list, lat_long), axis=1)

    return traffic_calming_list


def main():
    """
    Purpose:
        Test the function
    Args:
        N/A
    Returns:
        N/A
    """
    # Use the test data we have
    # df = pd.read_csv("../../data/analysis_data_roadway_blocks.csv")

    # 1400  - 1413 BLOCK OF SPRING ROAD NW

    # sample address
    address = "600 Farragut St. NW"
    print(f"Getting traffic calming features for {address}")

    text = get_traffic_calming(address)
    print(text)


if __name__ == "__main__":
    main()
