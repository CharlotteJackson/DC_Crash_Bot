# Python imports
import datetime
from typing import Union, Optional, Dict, Any


# 3rd Party Imports
import pandas as pd


def get_prev_requests(address: str) -> Dict[str, Any]:

    # TODO
    json_results = {}

    return json_results

# Once this works can update the main function to use database
def find_requests_in_past_12_months(df, address, curr_time):
    print("Have fun!!")


def main():
    """
    Purpose:
        Test the function
    Args:
        N/A
    Returns:
        N/A
    """
    print("hello")

    # Use the test data we have
    df = pd.read_csv("../../data/analysis_data_all311.csv")

    # sample address
    address = "BROAD BRANCH ROAD NW AND BRANDYWINE STREET NW"

    curr_time = datetime.datetime.now()

    # TODO given the address find all 311 requests in the past 12 months
    find_requests_in_past_12_months(df, address, curr_time)


if __name__ == "__main__":
    main()
