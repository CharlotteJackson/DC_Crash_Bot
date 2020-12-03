import tweepy
import datetime 
from connect_to_rds import get_connection_strings
import pprint 
import json
import boto3 
import os 

# get aws credentials
AWS_Credentials = get_connection_strings("AWS_DEV")
s3 = boto3.client('s3'
    ,aws_access_key_id=AWS_Credentials['aws_access_key_id']
    ,aws_secret_access_key=AWS_Credentials['aws_secret_access_key'])
s3_resource = boto3.resource('s3'
    ,aws_access_key_id=AWS_Credentials['aws_access_key_id']
    ,aws_secret_access_key=AWS_Credentials['aws_secret_access_key'])
bucket_name = AWS_Credentials['s3_bucket']
region=AWS_Credentials['region']
home = os.path.expanduser('~')
destination_folder='source-data/twitter/'
filename_stem = 'twitter_search_'

# get twitter credentials
consumer_key=get_connection_strings("Twitter")["API_Key"]
consumer_secret=get_connection_strings("Twitter")["API_Secret_Key"]
access_token=get_connection_strings("Twitter")["Access_Token"]
access_token_secret=get_connection_strings("Twitter")["Access_Token_Secret"]

# execute twitter search
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth,parser=tweepy.parsers.JSONParser())

search_terms_list =[
    'hit by car-filter:retweets'
    ,'hit by bus-filter:retweets'
    ,'hit by truck-filter:retweets'
    ,'ran red light-filter:retweets'
    ,'driver hit me-filter:retweets'
    ,'saw get hit-filter:retweets'
    ,'almost hit by-filter:retweets'
    ,'hit by driver-filter:retweets'
    ,'got run over-filter:retweets'
    ,'ran stop sign-filter:retweets'
    ,'almost hit me-filter:retweets'
    ,'nearly hit me-filter:retweets'
    ,'hit pedestrian-filter:retweets'
]

for term in search_terms_list:
    results=(api.search(q=term
                        ,geocode="38.93757635791988,-77.02422553222864,10mi"
                        ,result_type='recent'
                        ,include_entities=False
                        ,monitor_rate_limit=True 
                        ,wait_on_rate_limit=True
                        ,lang="en")
    )
    with open('data.json', 'w+') as outfile:
        json.dump(results['statuses'], outfile, indent=4)
    current_time = datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")
    destination_file=filename_stem+current_time
    upload = open('data.json', 'rb')
    s3_resource.Bucket(bucket_name).put_object(Key=destination_folder+destination_file, Body=upload, Metadata={"search_term":term})
 