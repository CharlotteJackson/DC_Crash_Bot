# Python imports
from typing import Union, Optional, Dict, Any


# 3rd Party Imports
import pandas as pd


def get_traffic_calming(address: str) -> Dict[str, Any]:

    # TODO
    json_results = {}

    return json_results


def find_traffic_calming_features(df, address):
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
    # Use the test data we have
    df = pd.read_csv("../../data/analysis_data_roadway_blocks.csv")

    # sample address
    address = "BROAD BRANCH ROAD NW AND BRANDYWINE STREET NW"

    # TODO given the address find all traffic calming features
    find_traffic_calming_features(df, address)


if __name__ == "__main__":
    main()
