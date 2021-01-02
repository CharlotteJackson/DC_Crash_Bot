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
import csv

# get aws credentials
AWS_Credentials = get_connection_strings("AWS_DEV")
client = boto3.client('s3'
    ,aws_access_key_id=AWS_Credentials['aws_access_key_id']
    ,aws_secret_access_key=AWS_Credentials['aws_secret_access_key'])
s3_resource = boto3.resource('s3'
    ,aws_access_key_id=AWS_Credentials['aws_access_key_id']
    ,aws_secret_access_key=AWS_Credentials['aws_secret_access_key'])
bucket = s3_resource.Bucket(AWS_Credentials['s3_bucket'])
bucket_name = AWS_Credentials['s3_bucket']
region=AWS_Credentials['region']
# set script variables
home = os.path.expanduser('~')
destination_folder='source-data/twitter/parsed/'
source_folder='source-data/twitter/all_time/'
current_date=datetime.datetime.now(timezone.utc).strftime("%Y-%m-%d")

# get timestamps of last query for each search term 
# last_queried = {obj.Object().metadata['search_term']:obj.Object().metadata['last_queried_at'] for obj in bucket.objects.filter(Prefix=destination_folder, Delimiter='/') if 'last_queried_at' in obj.Object().metadata.keys() if 'search_term' in obj.Object().metadata.keys()}
# superhero since:2015-12-21

# function to convert tweets to dataframe
def convert_tweets_to_df(tweets:list, df=None):
    statuses_list = {
                'search_term':[]
                ,'search_term_id':[]
                ,'convo_group_id':[]
                ,'tweet_id':[]
                ,'tweet_text':[]
                ,'reply_to_status':[]
                ,'quoted_status':[]
                ,'created_at':[]
                ,'user_id':[]
                ,'user_screen_name':[]
                ,'user_location':[]
                ,'coordinates':[]
                ,'user_place':[]
        }

    # parse out the desired columns
    for tweet in tweets:
        statuses_list['search_term'].append(tweet['search_term'])
        statuses_list['search_term_id'].append(tweet['search_term_id'])
        statuses_list['tweet_id'].append(tweet['id_str'])
        try:
            statuses_list['convo_group_id'].append(tweet['convo_group_id'])
        except KeyError:
            statuses_list['convo_group_id'].append('')
        # try to handle twitters absurd datetime format
        statuses_list['created_at'].append(datetime.datetime.strptime(tweet['created_at'],"%a %b %d %H:%M:%S %z %Y"))
        try:
            statuses_list['user_location'].append(tweet['user']['location'])
        except KeyError:
            statuses_list['user_location'].append('')
        try:    
            statuses_list['user_place'].append(tweet['user']['place'])
        except KeyError:
            statuses_list['user_place'].append('')
        statuses_list['user_id'].append(tweet['user']['id_str'])
        statuses_list['user_screen_name'].append(tweet['user']['screen_name'])
        try:
            statuses_list['coordinates'].append(tweet['coordinates'])
        except KeyError:
            statuses_list['coordinates'].append('')
        try:
            statuses_list['tweet_text'].append(tweet['extended_tweet']['full_text'])
        except KeyError:
            try:
                statuses_list['tweet_text'].append(tweet['full_text'])
            except KeyError:
                statuses_list['tweet_text'].append(tweet['text'])
        try:
            statuses_list['reply_to_status'].append(tweet['in_reply_to_status_id_str'])
        except KeyError:
            statuses_list['reply_to_status'].append('')
        try:
            statuses_list['quoted_status'].append(tweet['quoted_status_id_str'])
        except KeyError:
            statuses_list['quoted_status'].append('')

    # create the dataframe if it doesn't exist; append to it if it does
    if df is None or len(df)==0:
        df = pd.DataFrame.from_dict(statuses_list)
    else:
        newdf = pd.DataFrame.from_dict(statuses_list)
        df = df.append(newdf, ignore_index=True)
    return df

# get list of files to parse
files_to_parse = [obj.key for obj in bucket.objects.filter(Prefix=source_folder, Delimiter='/') if '.json' in obj.key]

# create empty dataframe to hold them
full_df = pd.DataFrame()

for file in files_to_parse:
    file_name = os.path.basename(file) 
    # load the json into memory
    f = client.get_object(Bucket = bucket_name, Key=file)
    # decode it as string 
    f2 = f['Body'].read()
    # .decode('utf-8')
    # load back into dictionary format 
    f3 = json.loads(f2)
    # parse the file into csv format
    full_df = convert_tweets_to_df(f3, full_df)

full_df=full_df.convert_dtypes(convert_string=True)

# save csv file
tmp_filename = Path(home, 'twitter_alltime.csv')
full_df.to_csv(tmp_filename, index=False, header=True, line_terminator='\n',float_format='%.2f',encoding='utf-8',quoting=csv.QUOTE_NONNUMERIC)
data = open(tmp_filename, 'rb')
bucket.put_object(Key=destination_folder+'twitter_alltime_'+current_date+'.csv', Body=data, Metadata={'endpoint':'all_time'
                                                                                                , "target_schema":'tmp', "target_table":'twitter'})