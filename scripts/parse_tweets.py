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
from get_address import GeoLoc
import argparse

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
GOOGLE_API_KEY = get_connection_strings("GOOGLE_MAPS")["API_Key"]

# set script variables
home = os.path.expanduser('~')
current_date=datetime.datetime.now(timezone.utc).strftime("%Y-%m-%d")

# get timestamps of last query for each search term 
# last_queried = {obj.Object().metadata['search_term']:obj.Object().metadata['last_queried_at'] for obj in bucket.objects.filter(Prefix=destination_folder, Delimiter='/') if 'last_queried_at' in obj.Object().metadata.keys() if 'search_term' in obj.Object().metadata.keys()}
# superhero since:2015-12-21


# function to convert tweets to dataframe
def convert_tweets_to_df(tweets:list, text_terms:list, df=None, geocode=False, source_file=''):


    if geocode == True:
        geo_loc_instance = GeoLoc(GOOGLE_API_KEY)

    statuses_list = {
                'search_term':[]
                ,'search_term_id':[]
                ,'convo_group_id':[]
                ,'tweet_id':[]
                ,'tweet_text':[]
                ,'tweet_place':[]
                ,'reply_to_status':[]
                ,'quoted_status':[]
                ,'retweeted_status':[]
                ,'created_at':[]
                ,'user_id':[]
                ,'user_screen_name':[]
                ,'user_location':[]
                ,'coordinates':[]
                ,'user_place':[]
                ,'GeoLoc_Lat':[]
                ,'GeoLoc_Long':[]
                ,'has_relevant_text':[]
                ,'relevant_text_search_terms':[]
                ,'source_file':[]
        }



    # turn list of filter terms into string
    text_terms_str = ' '.join([str(elem) for elem in text_terms])

    # parse out the desired columns
    for tweet in tweets:
        has_relevant_text = False

        # first grab the text 
        try:
            tweet_text = tweet['extended_tweet']['full_text']
        except KeyError:
            try:
                tweet_text = tweet['full_text']
            except KeyError:
                tweet_text = tweet['text']
        # then append to list
        statuses_list['tweet_text'].append(tweet_text)
        # then assign true or false
        if len(text_terms)>0:
            has_relevant_text = any(term.lower() in tweet_text.lower() for term in text_terms)
        else:
            has_relevant_text = False
        # and append that flag
        statuses_list['has_relevant_text'].append(has_relevant_text)
        # then geocode it if necessary
        if geocode == True:
            try:
                statuses_list['GeoLoc_Lat'].append(geo_loc_instance.GetGeoLoc(tweet_text)["lat"])
                statuses_list['GeoLoc_Long'].append(geo_loc_instance.GetGeoLoc(tweet_text)["lng"])
            except Exception as error:
                statuses_list['GeoLoc_Lat'].append('Could not geocode location')
                statuses_list['GeoLoc_Long'].append('Could not geocode location')
        else:
            statuses_list['GeoLoc_Lat'].append('')
            statuses_list['GeoLoc_Long'].append('')

        # append everything that's not relating to the text
        statuses_list['relevant_text_search_terms'].append(text_terms_str)
        try:
            statuses_list['search_term'].append(tweet['search_term'])
        except KeyError:
            statuses_list['search_term'].append('')
        try:
            statuses_list['search_term_id'].append(tweet['search_term_id'])
        except KeyError:
            statuses_list['search_term_id'].append('')
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
        try:    
            statuses_list['tweet_place'].append(tweet['place'])
        except KeyError:
            statuses_list['tweet_place'].append('')
        statuses_list['user_id'].append(tweet['user']['id_str'])
        try:
            statuses_list['user_screen_name'].append(tweet['user']['screen_name'])
        except:
            statuses_list['user_screen_name'].append('')
        try:
            statuses_list['coordinates'].append(tweet['coordinates'])
        except KeyError:
            statuses_list['coordinates'].append('')
        try:
            statuses_list['reply_to_status'].append(tweet['in_reply_to_status_id_str'])
        except KeyError:
            statuses_list['reply_to_status'].append('')
        try:
            statuses_list['quoted_status'].append(tweet['quoted_status_id_str'])
        except KeyError:
            statuses_list['quoted_status'].append('')
        try:
            statuses_list['retweeted_status'].append(tweet['retweeted_status']['id_str'])
        except KeyError:
            statuses_list['retweeted_status'].append('')
        statuses_list['source_file'].append(source_file)

    for key in statuses_list.keys():
        print(key,': ', len(statuses_list[key]))
    
    pprint.pprint(statuses_list)

    # create the dataframe if it doesn't exist; append to it if it does
    if df is None or len(df)==0:
        df = pd.DataFrame.from_dict(statuses_list)
    else:
        newdf = pd.DataFrame.from_dict(statuses_list)
        df = df.append(newdf, ignore_index=True)
    return df

def save_tweets_as_csv(source_folder = None, destination_folder=None,geocode_arg=False,text_terms_args = []):
    # get list of files to parse
    files_to_parse = [obj.key for obj in bucket.objects.filter(Prefix=source_folder) if '.json' in obj.key]
    print(files_to_parse)

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
        full_df = convert_tweets_to_df(tweets=f3, df=full_df,geocode=geocode_arg, source_file=file_name, text_terms=text_terms_args)

    full_df=full_df.convert_dtypes(convert_string=True)

    # save csv file
    tmp_filename = Path(home, 'twitter.csv')
    full_df.to_csv(tmp_filename, index=False, header=True, line_terminator='\n',float_format='%.2f',encoding='utf-8',quoting=csv.QUOTE_NONNUMERIC)
    data = open(tmp_filename, 'rb')
    bucket.put_object(Key=destination_folder+current_date+'.csv', Body=data, Metadata={"target_schema":'source_data', "target_table":'twitter'})


# set up ability to call with lists from the command line as follows:
# python parse_tweets.py --geocode True --source_folder source_data/twitter/specific_users/ --destination_folder source_data/twitter/specific_users/parsed/ --text_terms collision struck pedestrian crash auto traffic car cyclist vehicle bike truck suv accident bicycle
CLI=argparse.ArgumentParser()
CLI.add_argument(
"--text_terms",  
nargs="*",  
type=str,
default=['collision','struck', 'pedestrian', 'crash', 'auto', 'traffic', 'car', 'cyclist', 'vehicle', 'truck', 'suv', 'accident'],  # default - the two main twitter accounts that tweet out scanner audio
)
CLI.add_argument(
"--source_folder",   
type=str,
default='source_data/twitter/specific_users/'
)
CLI.add_argument(
"--destination_folder",   
type=str,
default='source_data/twitter/specific_users/parsed/'
)
CLI.add_argument(
"--geocode",   
type=bool,
default=False
)

# parse the command line
args = CLI.parse_args()
text_terms = args.text_terms
geocode_arg = args.geocode
source_folder_args=args.source_folder
destination_folder_args=args.destination_folder

if __name__=='__main__':
    print(source_folder_args)
    print(destination_folder_args)
    save_tweets_as_csv(source_folder=source_folder_args, destination_folder=destination_folder_args,geocode_arg=geocode_arg, text_terms_args=text_terms)