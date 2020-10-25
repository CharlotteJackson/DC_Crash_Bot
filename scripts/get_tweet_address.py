import os
import json
import logging

from get_address import GeoLoc


# set logging
logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(message)s", level=logging.INFO
)


GOOGLE_API_KEY = os.environ["GOOGLE_API_KEY"]


def example_use():

    address = "1200 Block of  Meigs Place , NE. DC"

    geo_loc_instance = GeoLoc(GOOGLE_API_KEY)
    lat_long = geo_loc_instance.GetGeoLoc(address)


def get_address_from_json(alert_dc_json):

    tweet_lat_long = []
    for tweet in alert_dc_json:

        tweet_lat_obj = {}

        logging.info(tweet["full_text"])
        address = tweet["full_text"]

        try:
            geo_loc_instance = GeoLoc(GOOGLE_API_KEY)
            lat_long = geo_loc_instance.GetGeoLoc(address)
        except Exception as error:
            logging.error(error)
            continue

        tweet_lat_obj["tweet"] = tweet["full_text"]
        tweet_lat_obj["google_geo"] = lat_long

        tweet_lat_long.append(tweet_lat_obj)

        with open("../data/AlertDCio_google_geo.json", "w") as outfile:
            json.dump(tweet_lat_long, outfile)


# TODO: allow you to pass in what handle to collect
def main():

    logging.info("Getting address from tweets")

    # hard coded

    with open("../data/AlertDCio.json") as json_file:
        alert_dc_json = json.load(json_file)

    get_address_from_json(alert_dc_json)
    # logging.info(alert_dc_json)


if __name__ == "__main__":
    main()
