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
bucket = s3_resource.Bucket(AWS_Credentials['s3_bucket'])
region=AWS_Credentials['region']
home = os.path.expanduser('~')
destination_folder='source-data/twitter/'
filename_stem = 'twitter_search_'

# get timestamps of last query for each search term 
last_queried = {obj.Object().metadata['search_term']:obj.Object().metadata['last_queried_at'] for obj in bucket.objects.filter(Prefix=destination_folder, Delimiter='/') if 'last_queried_at' in obj.Object().metadata.keys() if 'search_term' in obj.Object().metadata.keys()}
# superhero since:2015-12-21

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
    'hit by car -filter:retweets'
    ,'hit by bus -feel -feels -filter:retweets'
    ,'hit by truck -feel -feels -filter:retweets'
    ,'ran red light -filter:retweets'
    ,'driver hit me -filter:retweets'
    ,'saw get hit -filter:retweets'
    ,'almost hit by -filter:retweets'
    ,'hit by driver -filter:retweets'
    ,'got run over -reindeer -feel -filter:retweets'
    ,'got ran over -reindeer -feel -filter:retweets'
    ,'get run over -reindeer -feel -filter:retweets'
    ,'ran stop sign -filter:retweets'
    ,'almost hit me -filter:retweets'
    ,'nearly hit me -filter:retweets'
    ,'hit pedestrian -filter:retweets'
    ,'hit and run -filter:retweets'
    ,'pedestrian hit -filter:retweets'
    ,'bike hit -filter:retweets'
    ,'cyclist hit -filter:retweets'
    ,'in crosswalk -filter:retweets'
]

statuses_only =[]

for term in search_terms_list:
    if term in last_queried.keys():
        term_with_date=term+' since:'+last_queried[term]
    else:
        term_with_date=term
    results=(api.search(q=term_with_date
                        ,geocode="38.93757635791988,-77.02422553222864,10mi"
                        ,result_type='recent'
                        ,tweet_mode='extended'
                        ,include_entities=False
                        ,monitor_rate_limit=True 
                        ,wait_on_rate_limit=True
                        ,lang="en")
    )
    num_tweets=str(len(results['statuses']))
    for tweet in results['statuses']:
        statuses_only.append({tweet['id_str']:{"text":tweet['full_text'],"created_at":tweet['created_at'], "user_location":tweet['user']['location'],"search_term":term,"coordinates":tweet["coordinates"]}})
    with open('data.json', 'w+') as outfile:
        json.dump(results['statuses'], outfile, indent=4)
    current_time = datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")
    current_date=datetime.datetime.now().strftime("%Y-%m-%d")
    destination_file=filename_stem+current_time
    upload = open('data.json', 'rb')
    bucket.put_object(Key=destination_folder+destination_file+'.json', Body=upload, Metadata={"search_term":term, "last_queried_at":current_date,"num_tweets":num_tweets})

# upload just the statuses, user location, and search terms 
with open('data.json', 'w+') as outfile:
        json.dump(statuses_only, outfile, indent=4)
destination_file='all_statuses_'+current_time
upload = open('data.json', 'rb')
bucket.put_object(Key=destination_folder+destination_file+'.json', Body=upload, Metadata={"last_queried_at":current_date,"num_tweets":str(len(statuses_only))})
