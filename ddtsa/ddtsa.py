# Python imports
import json
import os
from typing import Union, Optional, Dict, Any
import requests
import logging

# 3rd Party Imports
import pandas as pd
import streamlit as st
from PIL import Image
from geopy import distance


# Project Imports
from data_collectors.get_address import rev_geocode
from data_collectors.unsafe_times import get_unsafe_times
from data_collectors.get_prev_311 import get_prev_requests
from data_collectors.traffic_calming import get_traffic_calming
from data_collectors.get_construction_projects import (
    get_nearby_construction_projects,
    format_incident_data,
)

# TODO might want to get google data just once
# # Check if google key found
try:
    GOOGLE_API_KEY = os.environ["GOOGLE_API_KEY"]
except Exception as error:
    st.error("No GOOGLE API KEY detected")


# Full results from Geocoding API for 14th St NW & Columbia Rd NW, Washington, DC 20009


def get_open_work_orders(lat, lng: str):
    """
    Purpose:
        Gets open work orders near address
    Args:
        address: Current Address
    Returns:
        N/A
    """
    # Documentation here https://developers.arcgis.com/python/
    # Hmm can we have the db do geometry for us?

    # TODO we will want to set up the cache from our db
    # api_url = "https://maps2.dcgis.dc.gov/dcgis/rest/services/DDOT/Cityworks/FeatureServer/2/query?f=json&where=((StatusClosed%20%3D%20%27NOT%20CLOSED%27)%20AND%20(HasWorkOrder%20%3D%20%27YES%27))%20AND%20(datetimeinit%3C%3Dtimestamp%20%272021-04-27%2003%3A59%3A59%27)&returnGeometry=true&spatialRel=esriSpatialRelIntersects&outFields=*&outSR=102100&resultOffset=0&resultRecordCount=500&resultType=standard"

    # TODO we may want to do some preproccessing if this data doesnt change much...
    # data = requests.get(api_url).json()["features"]

    curr_loc = (lat, lng)
    work_orders = data_collectors.utils.load_json("work_order.json")

    close_work_orders = []

    # Hmm I dont like work orders, can we get Waze instead?
    # TODO get waze data
    # https://developers.google.com/waze/data-feed/incident-information#json-feed-file
    for work_order in work_orders:

        try:

            # check if is washington dc
            # District of Columbia
            for result in work_order["gmap_data"]:

                # TODO is it always 3?
                if (
                    not result["address_components"][3]["long_name"]
                    == "District of Columbia"
                ):
                    continue

                work_lat = result["geometry"]["location"]["lat"]
                work_lng = result["geometry"]["location"]["lng"]
                # check if address is "near" one of these orders
                work_order_loc = (work_lat, work_lng)

                # Get current miles
                curr_dist = distance.distance(curr_loc, work_order_loc).miles
                # print(curr_dist)

                # TODO What should threshold be?
                work_order["distance"] = curr_dist
                if curr_dist < 0.6:
                    close_work_orders.append(work_order)

                # if curr_dist > 100:
                #     print(work_order)

                break
        except Exception as error:

            print(error)
            print(work_order)
            continue

    # Hmm do we want to just get xy coords?

    # TODO move this code to a weekly script for new data..
    # for datum in data:

    #     curr_addres = datum["attributes"]["probaddress"]
    #     gmap_data = geo_loc_instance.GetGeoLoc(curr_addres)
    #     print(gmap_data)
    #     datum["gmap_data"] = gmap_data

    # data_collectors.utils.save_json("work_order.json", data)
    return close_work_orders


def location_data(address: str, gmap_data: Dict[str, Any]):
    """
    Purpose:
        Generates Report
    Args:
        address: Current Address
    Returns:
        N/A
    """
    st.subheader(f"1. Location of requested investigation")
    st.write(
        "Define geographic boundaries as clearly as possible (400 block of A Street NE, the intersection of 1st Street & B Street NW, etc.)"
    )

    # lat_long = geo_loc_instance.GetGeoLoc(address)

    # st.write(lat_long)

    st.success(f"`{address}`")

    st.write(
        "Is this location near an existing construction project? If yes, please provide the name and location of the project and any construction‐related concerns."
    )

    const_data = get_nearby_construction_projects(gmap_data)
    st.markdown(format_incident_data(const_data))


def safety_concerns(address: str):
    """
    Purpose:
        Get safety concerns
    Args:
        address: Current Address
    Returns:
        N/A
    """
    st.subheader("2. Safety concerns")

    st.write(
        "Provide a detailed description of the problems observed in the area of investigation (vehicle crashes, speeding, pedestrian safety, bicycle safety, unable to cross the street, hard to see cross‐traffic, etc.) For intersection‐related concerns, please include the type of intersection:"
    )


def time_of_day_concerns(address: str, gmap_data: Dict[str, Any]):
    """
    Purpose:
        Get time of day concerns
    Args:
        address: Current Address
    Returns:
        N/A
    """
    st.subheader("3. Days and time when safety concerns are the worst:")

    st.write("(Such as weekday AM peak, weekday PM peak, overnight, weekends, etc.) ")

    data = get_unsafe_times(address, gmap_data)

    st.markdown(data)


def existing_traffic_calms(address: str, gmap_data: Dict[str, Any]):
    """
    Purpose:
        Get existing traffic calming features
    Args:
        address: Current Address
    Returns:
        N/A
    """
    st.subheader("4. Are there existing traffic calming features on the block?")

    st.write("This includes speed humps, rumble strips, etc.")

    data = get_traffic_calming(address, gmap_data)

    st.write(data)


def get_neighborhood_uses(address: str):
    """
    Purpose:
        Describe neighborhood uses:
    Args:
        address: Current Address
    Returns:
        N/A
    """
    st.subheader("5. Describe neighborhood uses ")

    st.write(
        "Such as residential area, retail area, school zone, recreation center, community center, etc."
    )


def get_multi_modal(address: str):
    """
    Purpose:
        Get multi‐modal facilities
    Args:
        address: Current Address
    Returns:
        N/A
    """
    st.subheader("6. Describe multi‐modal facilities")

    st.write(
        "Are there sidewalks? Bike facilities or trails? Nearby Metrorail station or Metrobus stop(s)?"
    )


def get_vehicle_types(address: str):
    """
    Purpose:
        Get Vehicle types
    Args:
        address: Current Address
    Returns:
        N/A
    """
    st.subheader("7. Vehicle types:")

    st.write(
        "Is the concern about commuter traffic in cars? Is there a high volume of trucks, perhaps due to nearby construction? What about buses?"
    )


def get_prev_concerns(address: str, gmap_data: Dict[str, Any]):
    """
    Purpose:
        Get previous concerns
    Args:
        address: Current Address
    Returns:
        N/A
    """
    st.subheader("8. Have you previously contacted DDOT about your concerns?")

    st.write("8. Have you previously contacted DDOT about your concerns?")

    data = get_prev_requests(address, gmap_data)

    st.write(data)


def get_extra_info(address: str):
    """
    Purpose:
        Get extra info
    Args:
        address: Current Address
    Returns:
        N/A
    """
    st.subheader("9. Any other information you would like to share?")

    st.write("We are awesome!!!!!")


def generate_report(address: str):
    """
    Purpose:
        Generates Report
    Args:
        address: Current Address
    Returns:
        N/A
    """
    st.header(f"Generating report for {address}")
    # TODO may want to check if address is in DC
    # TODO we should input the google maps data into these functions
    gmap_data = rev_geocode(address, GOOGLE_API_KEY)
    # st.write(gmap_data)

    location_data(address, gmap_data)
    safety_concerns(address)
    time_of_day_concerns(address, gmap_data)
    existing_traffic_calms(address, gmap_data)
    get_neighborhood_uses(address)
    get_multi_modal(address)
    get_vehicle_types(address)
    get_prev_concerns(address, gmap_data)
    get_extra_info(address)


def sidebar():
    """
    Purpose:
        Shows the app sidebar
    Args:
        N/A
    Returns:
        N/A
    """
    # TODO get better image
    st.sidebar.image(
        "https://codefordc.org/images/logo/code-for-dc-logo-text-bottom.png"
    )

    st.sidebar.write(
        "Follow our progress on Github https://github.com/CharlotteJackson/DC_Crash_Bot"
    )


def app():
    """
    Purpose:
        Controls the app flow
    Args:
        N/A
    Returns:
        N/A
    """
    sidebar()

    st.header("Data-Driven Traffic Safety Generator")

    st.write(
        "The goal is to utilize data to automatically generate a [traffic safety assessment document](https://ddot.dc.gov/sites/default/files/dc/sites/ddot/service_content/attachments/2019%20Traffic%20Safety%20Assessment%20Questionnaire%20%28002%29%20%28003%29.pdf)"
    )

    st.write("Simply enter in an address and watch the results populate")

    address = st.text_input("Address")

    if st.button("Generate Report"):
        with st.spinner():
            # Run everything !!!!
            generate_report(address)

        # Done and Done
        st.balloons()


def main():
    """
    Purpose:
        Controls the flow of the streamlit app
    Args:
        N/A
    Returns:
        N/A
    """
    app()


if __name__ == "__main__":
    main()
