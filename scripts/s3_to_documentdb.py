import boto3
import connect_to_mongo
import os


AWS_Credentials = connect_to_mongo.get_connection_strings("AWS_DEV")
s3 = boto3.client('s3'
    ,aws_access_key_id=AWS_Credentials['aws_access_key_id']
    ,aws_secret_access_key=AWS_Credentials['aws_secret_access_key'])
s3_resource = boto3.resource('s3'
    ,aws_access_key_id=AWS_Credentials['aws_access_key_id']
    ,aws_secret_access_key=AWS_Credentials['aws_secret_access_key'])
bucket = s3.Bucket(AWS_Credentials['s3_bucket'])
region=AWS_Credentials['region']
home = os.path.expanduser('~')
client = connect_to_mongo.MongoDB_Client(destination="AWS_DocumentDB", env="DEV",dbName="pulsepoint")

# set which S3 folder(s) to load data for
folders_to_load = ['source-data/pulsepoint/']

for folder in folders_to_load:
    # grab list of all csv files in target folder
    files_to_load = [(obj.key.replace(folder,'')) for obj in bucket.objects.filter(Prefix=folder, Delimiter='/') if '.json' in obj.key]