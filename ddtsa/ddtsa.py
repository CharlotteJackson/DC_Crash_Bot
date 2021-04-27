import base64
import glob
import json
import os
import uuid
from typing import Union, Optional
import requests

import pandas as pd
import streamlit as st
from PIL import Image


def get_open_work_orders(address: str):
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
    api_url = "https://maps2.dcgis.dc.gov/dcgis/rest/services/DDOT/Cityworks/FeatureServer/2/query?f=json&where=((StatusClosed%20%3D%20%27NOT%20CLOSED%27)%20AND%20(HasWorkOrder%20%3D%20%27YES%27))%20AND%20(datetimeinit%3C%3Dtimestamp%20%272021-04-27%2003%3A59%3A59%27)&returnGeometry=true&spatialRel=esriSpatialRelIntersects&outFields=*&outSR=102100&resultOffset=0&resultRecordCount=500&resultType=standard"

    # TODO check if address is "near" one of these orders
    # TODO we may want to do some preproccessing if this data doesnt change much...
    data = requests.get(api_url).json()["features"]
    return data


def location_data(address: str):
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

    st.success(f"`{address}`")

    st.write(
        "Is this location near an existing construction project? If yes, please provide the name and location of the project and any construction‐related concerns."
    )

    # Uncomment until ready to work on it
    # st.write(get_open_work_orders(address))


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
        "Provide a detailed description of the problems observed in the area of investigation (vehicle crashes, speeding, pedestrian safety, bicycle safety, unable to cross the street, hard to see cross‐traffic, etc.)For intersection‐related concerns, please include the type of intersection:"
    )


def time_of_day_concerns(address: str):
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


def existing_traffic_calms(address: str):
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


def get_prev_concerns(address: str):
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
    location_data(address)
    safety_concerns(address)
    time_of_day_concerns(address)
    existing_traffic_calms(address)
    get_neighborhood_uses(address)
    get_multi_modal(address)
    get_vehicle_types(address)
    get_prev_concerns(address)
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