# Python imports
from typing import Union, Optional, Dict, Any


# 3rd Party Imports
import pandas as pd


def get_unsafe_times(address: str) -> Dict[str, Any]:

    # TODO
    json_results = {}

    return json_results


def find_unsafe_times(df, address):
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
    df = pd.read_csv("../../data/analysis_data_dc_crashes_w_details.csv")

    # sample address
    address = "BROAD BRANCH ROAD NW AND BRANDYWINE STREET NW"

    # TODO given the address find what times crashes happen
    find_unsafe_times(df, address)


if __name__ == "__main__":
    main()
