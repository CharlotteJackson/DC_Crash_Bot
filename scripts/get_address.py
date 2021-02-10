import urllib.parse
import os
import requests
# Make sure to pip install requests to get this to work
# https://requests.readthedocs.io/en/master/


class GeoLoc:

    def __init__(self, token):
        self.google_key = token

    def GetGeoLoc(self, test_address):
        url = "https://maps.googleapis.com/maps/api/geocode/json?address={final_address}&key={GOOGLE_KEY}".format(
            final_address=test_address, GOOGLE_KEY=self.google_key)
        # print(url)

        r = requests.get(url)
        # print(r.json())
        return r.json()['results'][0]['geometry']['location']


# TODO Use Env vars instead for the Google geolocation API
# Do not commit tokens to github !


def TestFuc():
    # TODO Use Env vars instead for the Google geolocation API
    # Do not commit tokens to github !
    api_token = None
    try:
        dir_path = os.path.dirname(os.path.realpath(__file__))
        path = os.path.join(dir_path, "api_key.txt")
        print(path)
        api_key_file = open(path, 'r')

        api_token = api_key_file.read()
    except Exception as identifier:
        api_token = os.environ['google_api_token']

    test_tweet = "DC Water reports the 1200 block of Missouri Avenue NW, between 13th Street and Colorado Avenue NW, will be closed f http://alertdc.io/t/6Zb"
    print(test_tweet)

    geo_loc_instance = GeoLoc(api_token)
    lat_long = geo_loc_instance.GetGeoLoc(test_tweet)
    print(lat_long)


# TestFuc()
