# Python imports
import logging
import os
from typing import Tuple, Union, Optional, Dict, Any
import requests

# 3rd Party Imports
from datetime import datetime
from geopy.distance import geodesic
import pandas as pd

# Project Imports


# Check if bing key found
try:
    BING_API_KEY = os.environ["BING_API_KEY"]
except Exception as error:
    logging.error("No BING API KEY detected")


def format_incident_data(incident_json: Dict[str, Any]):
    """
    Purpose:
        Format incidents data
    Args:
        incident_json: Data from bing maps
    Returns:
        formated incident data
    """

    incident_string = "Here are the latests incidents in the area...   \n"

    for incident in incident_json["resourceSets"][0]["resources"]:
        incident_string += f'\n {incident["description"]}   '

    if len(incident_json["resourceSets"][0]["resources"]) == 0:
        incident_string = "No local incidents"

    return incident_string


def get_nearby_construction_projects(gmap_data: Dict[str, Any]):
    """
    Purpose:
        Get Incidents data given gmap data
    Args:
        gmap_data: Data from google maps
    Returns:
        json data for Incidents
    """
    south_lat = gmap_data[0]["geometry"]["viewport"]["southwest"]["lat"]
    south_lng = gmap_data[0]["geometry"]["viewport"]["southwest"]["lng"]
    north_lat = gmap_data[0]["geometry"]["viewport"]["northeast"]["lat"]
    north_lng = gmap_data[0]["geometry"]["viewport"]["northeast"]["lng"]

    lat_long_string = f"{south_lat},{south_lng},{north_lat},{north_lng}"

    url = f"http://dev.virtualearth.net/REST/v1/Traffic/Incidents/{lat_long_string}?key={BING_API_KEY}"
    print(url)
    r = requests.get(url)
    print(r.json())
    # return r.json()['results'][0]['geometry']['location']
    return r.json()


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

    print(f"Testing using address: {address}")

    # TODO given the address find what times crashes happen
    construction_data = get_nearby_construction_projects(address)

    print(construction_data)


if __name__ == "__main__":
    main()
