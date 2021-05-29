# Python imports
from typing import Union, Optional, Dict, Any


# 3rd Party Imports
import pandas as pd


def get_traffic_calming(address: str) -> Dict[str, Any]:

    # TODO
    json_results = {}

    return json_results


def format_traffic_calming_json(traffic_calming_json: Dict[str, Any]):
    """
    Purpose:
        Format the json to a text response
    Args:
        traffic_calming_json: traffic calmaing data
    Returns:
        text: formatted text
    """
    text = ""

    text += f'There are {traffic_calming_json["totalbikelanes"]} bike lanes  \n'
    text += f'There are {traffic_calming_json["totalraisedbuffers"]} raised buffers  \n'
    text += f'There are {traffic_calming_json["totalparkinglanes"]} parking lanes  \n'
    text += f'The speed limit is {traffic_calming_json["speedlimit"]} MPH  \n'

    text += f"There are TODO speed cameras \n"
    text += f"There are TODO speed humps \n"

    return text


# TODO pass in google map data gmap_data: Dict[str, Any]


def find_traffic_calming_features(df: pd.DataFrame, address: str) -> Dict[str, Any]:
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

    road_way_blocks = df.iloc[0]

    # TODO
    # speed_cameras = speed_camera_df
    # speed_humps = speed_humps df

    # speedlimit
    traffic_calming_json = {}
    traffic_calming_json["speedlimit"] = road_way_blocks["speed_limit"]

    traffic_calming_json["num_sides_w_sidewalks"] = road_way_blocks[
        "num_sides_w_sidewalks"
    ]
    traffic_calming_json["totalbikelanes"] = road_way_blocks["totalbikelanes"]
    traffic_calming_json["totalraisedbuffers"] = road_way_blocks["totalraisedbuffers"]
    traffic_calming_json["totalparkinglanes"] = road_way_blocks["totalparkinglanes"]

    return traffic_calming_json


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
    df = pd.read_csv("../../data/analysis_data_roadway_blocks.csv")

    # 1400  - 1413 BLOCK OF SPRING ROAD NW

    # sample address
    address = "1400 Spring Rd NW"
    print(f"Getting traffic calming features for {address}")

    # TODO given the address find all traffic calming features
    tc_json = find_traffic_calming_features(df, address)
    print(format_traffic_calming_json(tc_json))


if __name__ == "__main__":
    main()
