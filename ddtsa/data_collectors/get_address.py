import urllib.parse
import logging
import os
from typing import Union, Optional, Dict, Any
import requests


def rev_geocode(address: str, api_key: str) -> Dict[str, Any]:
    """
    Purpose:
        Get google maps data from address
    Args:
        address - address to use
        api_key - google api key
    Returns:
        results json - google maps data for address
    """

    url = "https://maps.googleapis.com/maps/api/geocode/json?address={final_address}&key={GOOGLE_KEY}".format(
        final_address=address, GOOGLE_KEY=api_key
    )
    # print(url)
    r = requests.get(url)
    # print(r.json())
    # return r.json()['results'][0]['geometry']['location']
    return r.json()["results"]


def main():
    GOOGLE_API_KEY = os.environ["GOOGLE_API_KEY"]

    test_address = "Trinity Towers Apartments, 3023 14th St NW, Washington, DC 20009"
    print(test_address)

    gmap_data = rev_geocode(test_address, GOOGLE_API_KEY)
    print(gmap_data)


if __name__ == "__main__":
    main()
