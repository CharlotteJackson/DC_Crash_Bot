# Python imports
import os
from typing import Dict, Any

# 3rd Party Imports
import streamlit as st

# from PIL import Image


# Project Imports
from data_collectors.get_address import rev_geocode
from data_collectors.unsafe_times import get_unsafe_times
from data_collectors.get_prev_311 import get_prev_requests
from data_collectors.traffic_calming import get_traffic_calming
from data_collectors.waze_scrapper import get_waze_data
from data_collectors.get_crash_data import get_safety_concerns
from data_collectors.get_anc import get_anc_info
from data_collectors.neighborhood_stats import get_neighborhood_data
from data_collectors.get_multi_modal import get_multi_modal_data


from data_collectors.get_construction_projects import (
    get_nearby_construction_projects,
    format_incident_data,
)

# Check if google key found
try:
    GOOGLE_API_KEY = os.environ["GOOGLE_API_KEY"]
except Exception as error:
    st.error("No GOOGLE API KEY detected")


def location_data(address: str, gmap_data: Dict[str, Any]):
    """
    Purpose:
        Generates Report
    Args:
        address: Current Address
        gmap_data: google maps data
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

    const_data = get_nearby_construction_projects(gmap_data)
    st.markdown(format_incident_data(const_data))


def safety_concerns(address: str, gmap_data: Dict[str, Any]):
    """
    Purpose:
        Get safety concerns
    Args:
        address: Current Address
        gmap_data: google maps data
    Returns:
        N/A
    """
    st.subheader("2. Safety concerns")

    st.write(
        "Provide a detailed description of the problems observed in the area of investigation (vehicle crashes, speeding, pedestrian safety, bicycle safety, unable to cross the street, hard to see cross‐traffic, etc.) For intersection‐related concerns, please include the type of intersection:"
    )

    waze_data = get_waze_data(address, gmap_data)

    safety_data = get_safety_concerns(address, gmap_data)

    st.markdown(safety_data)

    with st.beta_expander("Waze Data"):
        st.markdown(waze_data)


def time_of_day_concerns(address: str, gmap_data: Dict[str, Any]):
    """
    Purpose:
        Get time of day concerns
        gmap_data: google maps data
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
        gmap_data: google maps data
    Returns:
        N/A
    """
    st.subheader("4. Are there existing traffic calming features on the block?")

    st.write("This includes speed humps, rumble strips, etc.")

    data = get_traffic_calming(address, gmap_data)

    st.write(data)


def get_neighborhood_uses(address: str, gmap_data: Dict[str, Any]):
    """
    Purpose:
        Describe neighborhood uses:
    Args:
        address: Current Address
        gmap_data: google maps data
    Returns:
        N/A
    """
    st.subheader("5. Describe neighborhood uses ")

    st.write(
        "Such as residential area, retail area, school zone, recreation center, community center, etc."
    )

    data = get_neighborhood_data(address, gmap_data)
    st.write(data)


def get_multi_modal(address: str, gmap_data: Dict[str, Any]):
    """
    Purpose:
        Get multi‐modal facilities
    Args:
        address: Current Address
        gmap_data: google maps data
    Returns:
        N/A
    """
    st.subheader("6. Describe multi‐modal facilities")

    st.write(
        "Are there sidewalks? Bike facilities or trails? Nearby Metrorail station or Metrobus stop(s)?"
    )

    data = get_multi_modal_data(address, gmap_data)
    st.write(data)


def get_vehicle_types(address: str):
    """
    Purpose:
        Get Vehicle types
    Args:
        address: Current Address
        gmap_data: google maps data
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
        gmap_data: google maps data
    Returns:
        N/A
    """
    st.subheader("8. Have you previously contacted DDOT about your concerns?")

    data = get_prev_requests(address, gmap_data)
    st.write(data)


def get_extra_info(address: str):
    """
    Purpose:
        Get extra info
    Args:
        address: Current Address
        gmap_data: google maps data

    Returns:
        N/A
    """
    st.subheader("9. Any other information you would like to share?")

    st.write("We are awesome!!!!!")


def show_anc(address: str, gmap_data: Dict[str, Any]):
    """
    Purpose:
        Get anc info
    Args:
        address: Current Address
        gmap_data: google maps data

    Returns:
        N/A
    """
    data = get_anc_info(address, gmap_data)

    st.subheader(data["name"])
    st.markdown("**You must have an ANC Commissioner sign off on your TSA**")
    st.write(f"Here is the ANC website for {address}")
    st.write(data["website"])


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
    gmap_data = rev_geocode(address, GOOGLE_API_KEY)

    show_anc(address, gmap_data)
    location_data(address, gmap_data)
    safety_concerns(address, gmap_data)
    time_of_day_concerns(address, gmap_data)
    existing_traffic_calms(address, gmap_data)
    get_neighborhood_uses(address, gmap_data)
    get_multi_modal(address, gmap_data)
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
