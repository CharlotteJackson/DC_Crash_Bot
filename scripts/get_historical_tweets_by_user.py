import tweepy
import datetime 
from datetime import timezone
import time
from connect_to_rds import get_connection_strings
import pprint 
import json
import boto3 
import os 
import pandas as pd
from pathlib import Path
import argparse
import pprint

# get aws credentials
AWS_Credentials = get_connection_strings("AWS_DEV")
s3 = boto3.client('s3'
    ,aws_access_key_id=AWS_Credentials['aws_access_key_id']
    ,aws_secret_access_key=AWS_Credentials['aws_secret_access_key'])
s3_resource = boto3.resource('s3'
    ,aws_access_key_id=AWS_Credentials['aws_access_key_id']
    ,aws_secret_access_key=AWS_Credentials['aws_secret_access_key'])
bucket = s3_resource.Bucket(AWS_Credentials['s3_bucket'])
region=AWS_Credentials['region']
# set script variables
home = os.path.expanduser('~')
destination_folder='source-data/twitter/specific_users/'
filename_stem = 'twitter_history_'

environment_name ='dev'
current_date=datetime.datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00:00")

# get twitter credentials and authenticate
consumer_key=get_connection_strings("Twitter")["API_Key"]
consumer_secret=get_connection_strings("Twitter")["API_Secret_Key"]
access_token=get_connection_strings("Twitter")["Access_Token"]
access_token_secret=get_connection_strings("Twitter")["Access_Token_Secret"]

def get_all_tweets(screen_name):

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth,parser=tweepy.parsers.JSONParser())
    #Twitter only allows access to a users most recent 3240 tweets with this method
    
    #initialize a list to hold all the tweepy Tweets
    alltweets = []  
    
    #make initial request for most recent tweets (200 is the maximum allowed count)
    new_tweets = api.user_timeline(screen_name = screen_name,count=200,tweet_mode='extended')
    
    #save most recent tweets
    alltweets.extend(new_tweets)

    #save the id of the oldest tweet less one
    oldest = alltweets[-1]['id'] - 1
    
    #keep grabbing tweets until there are no tweets left to grab
    while len(new_tweets) > 0:
        print(f"getting tweets before {oldest}")
        
        #all subsiquent requests use the max_id param to prevent duplicates
        new_tweets = api.user_timeline(screen_name = screen_name,count=200,max_id=oldest,tweet_mode='extended')
        
        #save most recent tweets
        alltweets.extend(new_tweets)
        
        #update the id of the oldest tweet less one
        oldest = alltweets[-1]['id'] - 1
        
        print(f"...{len(alltweets)} tweets downloaded so far")
    
    # set the list of tweets to just be the results

    upload=json.dumps(alltweets,indent=4)
    destination_file=filename_stem+screen_name+'_'+current_date
    bucket.put_object(Key=destination_folder+destination_file+'.json', Body=upload, Metadata={"user_id":screen_name 
                                                                                            , "last_queried_at":current_date,"num_tweets":str(len(alltweets))})
    print(destination_folder+destination_file+'.json', ' successfully uploaded to S3')


# set up ability to call with lists from the command line as follows:
# python get_historical_tweets_by_user.py --users alanhenney realtimenews10
CLI=argparse.ArgumentParser()
CLI.add_argument(
"users",  
nargs="*",  
type=str,
default=['alanhenney','realtimenews10'],  # default - the two main twitter accounts that tweet out scanner audio
)

# parse the command line
args = CLI.parse_args()
users = args.users

for user in users:
    get_all_tweets(user)