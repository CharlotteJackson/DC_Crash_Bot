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
destination_folder='source-data/twitter/all_time/'
filename_stem = 'twitter_search_'
date_tuples=[("201501010000","202012310000")]

environment_name ='dev'
current_date=datetime.datetime.now(timezone.utc).strftime("%Y-%m-%d")

# get twitter credentials and authenticate
consumer_key=get_connection_strings("Twitter")["API_Key"]
consumer_secret=get_connection_strings("Twitter")["API_Secret_Key"]
access_token=get_connection_strings("Twitter")["Access_Token"]
access_token_secret=get_connection_strings("Twitter")["Access_Token_Secret"]
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth,parser=tweepy.parsers.JSONParser())


search_terms_list_premium ={
    '01':'(hit OR struck OR clipped) by (car OR bus OR truck OR driver OR suv) -feel -feeling -feels',
    '02':'(car OR bus OR truck OR driver OR suv) (hit OR clipped OR struck) (me OR someone OR toddler OR kid OR teenager OR person OR man OR woman OR adult OR preschooler OR child) -food -feeling -feel -feels'
    ,'03':'(car OR bus OR truck OR driver OR suv) (run OR ran) over -feel -feeling -feels'
    ,'04':'(almost OR nearly OR "narrowly avoided") (run OR ran OR running) over'
    ,'05':'(almost OR nearly OR "narrowly avoided") (hit OR hitting) (by OR me)'
    ,'12':'(dangerous OR horrible OR bad OR awful OR worst OR scary OR terrible) (intersection OR crosswalk OR road OR street)'
    ,'09':'("hit and run" OR "hit-and-run") (pedestrian OR intersection OR car OR driver OR accident OR crash)'
    ,'10':'(pedestrian OR bike OR cyclist OR biker) (hit OR clipped OR struck)'
    ,'08':'ran ("stop sign" OR "red light")'
    ,'11':'in (a OR the) crosswalk'
    ,'06':'(#VisionZero OR "vision zero" OR "visionzero") (bowser OR ddot OR dc OR ddotdc)'
}

query_parameters = [
     '(point_radius:[-77.02422 38.93757 10.0mi] OR profile_locality:"Washington, D. C.")'#search within 10 miles of petworth metro station and/or people with 'dc' in their profile bio
     ,'-khyatt2876 -skooch8 -trump -@AOC -@chrissyteigen -@realcandaceo -@realdonaldtrump -biden -interception -defense -offense' # the terms and people to exclude from every query 
     ,'-karon -hylton' # the terms and people to exclude from every query 
     ,'-is:retweet'
     ,'has:profile_geo'
     ,'-cyber -game'
     ,'profile_country:"US"'
 ]

# function to get all replies/quote tweets from a thread
def get_recursive_replies(tweets_to_parse:list, search_term:str, search_term_id:str,convo_group_id=None, master_list=[]):
    for tweet in tweets_to_parse:
        if convo_group_id is None: 
            tweet['convo_group_id'] = tweet['id_str']
        else:
            tweet['convo_group_id'] = convo_group_id
        tweet['search_term'] = search_term
        tweet['search_term_id'] = search_term_id
        master_list.append(tweet)
        new_tweets_to_search=[]
        if 'in_reply_to_status_id_str' in tweet.keys() and tweet['in_reply_to_status_id'] is not None:
            try:
                new_tweets_to_search.append(api.get_status(tweet['in_reply_to_status_id'], include_entities = False,tweet_mode='extended'))
            except:
                print('status id ',tweet['in_reply_to_status_id'], 'could not be found')
        if 'quoted_status_id_str' in tweet.keys() and tweet['quoted_status_id'] is not None:
            try:
                new_tweets_to_search.append(api.get_status(tweet['quoted_status_id'], include_entities = False,tweet_mode='extended'))
            except:
                print('status id ',tweet['quoted_status_id'], 'could not be found')
        if len(new_tweets_to_search)>0:
            if convo_group_id is None:
                convo_group_id=tweet['id_str']
            get_recursive_replies(new_tweets_to_search, convo_group_id = convo_group_id, search_term=search_term, search_term_id=search_term_id, master_list=master_list)
        else:
            continue

    return master_list


for (search_fromDate, search_toDate) in date_tuples:

    for term_id, term in search_terms_list_premium.items():

        page=0
        query=term
        for parameter in query_parameters:
            query+=' '+parameter
        response=api.search_full_archive(environment_name, query, fromDate=search_fromDate, toDate = search_toDate, maxResults=500)
        
        # get the tweets that aren't retweets
        tweets_to_parse=[tweet for tweet in response['results'] if 'retweeted_status' not in tweet.keys()]
        print(len(tweets_to_parse)," tweets found")
        # attempt to get quotes and replies to bring all the relevant text into one view
        tweets_to_parse_with_quotes_and_replies = get_recursive_replies(tweets_to_parse, search_term=term, search_term_id=term_id,convo_group_id=None,master_list=[])
        print(len(tweets_to_parse_with_quotes_and_replies)," tweets found including replies and retweets")
        # save each page of results to S3
        upload=json.dumps(tweets_to_parse_with_quotes_and_replies,indent=4)
        destination_file=filename_stem+'searchterm_'+term_id+'_page_'+str(page)+'_'+current_date
        bucket.put_object(Key=destination_folder+destination_file+'.json', Body=upload, Metadata={"search_term":term, "serch_term_id": term_id, 'endpoint':'all_time'
                                                                                                , "last_queried_at":current_date,"num_tweets":str(len(tweets_to_parse_with_quotes_and_replies))})
        print(destination_folder+destination_file+'.json', ' successfully uploaded to S3')
        # results=api.search_full_archive(environment_name, query, fromDate=date_since_pro, maxResults=500)

        time.sleep(2)
        if 'next' in response.keys():
            # ugh, why did they make the response sometimes a tuple and sometimes a dict
            while True:
                page+=1
                if isinstance(response,dict):
                    next=response['next'] 
                else:
                    next=response[0]['next'] 
                response=api.search_full_archive(environment_name, query, fromDate=search_fromDate, toDate = search_toDate, maxResults=500, next=next)
                # response=api.search_30_day(environment_name, query, maxResults=100, next=next)

                tweets_to_parse=[tweet for tweet in response[0]['results'] if 'retweeted_status' not in tweet.keys()]
                print(len(tweets_to_parse_with_quotes_and_replies)," tweets found including replies and retweets")

                # get all the replies and quote tweets so the conversations make more sense
                tweets_to_parse_with_quotes_and_replies = get_recursive_replies(tweets_to_parse, search_term=term, search_term_id=term_id,convo_group_id=None,master_list=[])
                print(len(tweets_to_parse_with_quotes_and_replies)," tweets found including replies and retweets")

                upload=json.dumps(tweets_to_parse_with_quotes_and_replies,indent=4)
                destination_file=filename_stem+'searchterm_'+term_id+'_page_'+str(page)+'_'+current_date
                bucket.put_object(Key=destination_folder+destination_file+'.json', Body=upload, Metadata={"search_term":term, "serch_term_id": term_id, 'endpoint':'all_time'
                                                                                                    , "last_queried_at":current_date,"num_tweets":str(len(tweets_to_parse_with_quotes_and_replies))})
                print(destination_folder+destination_file+'.json', ' successfully uploaded to S3')
                time.sleep(2)
                if (isinstance(response,tuple) and 'next' not in response[0].keys()) or (isinstance(response,dict) and 'next' not in response.keys()):
                    break
        elif 'next' not in response.keys():
            continue
