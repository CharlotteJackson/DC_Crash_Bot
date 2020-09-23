import os
import json
import pandas as pd
import logging

import twitter



CONSUMER_KEY=os.environ['CONSUMER_KEY']
CONSUMER_SECRET=os.environ['CONSUMER_SECRET']
ACCESS_TOKEN_KEY=os.environ['ACCESS_TOKEN_KEY']
ACCESS_TOKEN_SECRET=os.environ['ACCESS_TOKEN_SECRET']


logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

api = twitter.Api(consumer_key=CONSUMER_KEY,
                  consumer_secret=CONSUMER_SECRET,
                  access_token_key=ACCESS_TOKEN_KEY,
                  access_token_secret=ACCESS_TOKEN_SECRET, tweet_mode='extended', sleep_on_rate_limit=True)


def make_twitter_json_output(user):
        tweet_json = api.GetUserTimeline(screen_name=user,count=100)
        tweet_data = []
        for tweet in tweet_json:
            tweet_data.append(tweet._json)
        with open("data/"+user+'.json', 'w') as outfile:
            json.dump(tweet_data, outfile)



def main():

    logging.info("Starting twitter collection")

    #enter user here

    user = "AlertDCio"
    make_twitter_json_output(user)

    logging.info("Jobs done")



if __name__ == "__main__":
    main()