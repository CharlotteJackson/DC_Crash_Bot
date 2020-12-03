#################################
## all MongoDB related functions
#################################

import pymongo
import datetime
import json
import urllib 
import os

def get_user():
    
    User = next((os.environ[k] for k in ['USER','USERNAME','LOGNAME'] if k in os.environ.keys()), 'UNKNOWN')
    
    return User 

def get_connection_strings(destination: str):

    # create empty dictionary to hold connection strings
    connection_info = {}

    home = os.path.expanduser('~')

    # generate full file path
    connection_file = os.path.join(home, 'credentials.json')

    with open(connection_file, 'r') as fhand:
        data = json.load(fhand)
        for key in data.keys():
            if key == destination:
                connection_info = data[key]

    return connection_info

# open connection to MongoDB
def MongoDB_Client(destination: str, env:str, dbName = None):

    credentials = get_connection_strings(destination)

    credentials[env]['UID'] = urllib.parse.quote(credentials[env]['UID'])
    credentials[env]['PWD'] = urllib.parse.quote(credentials[env]['PWD'])
    credentials[env]['URL'] = urllib.parse.quote(credentials[env]['mydbcluster'])

    client = pymongo.MongoClient("mongodb://{}:{}@{}.us-east-1.docdb.amazonaws.com:27017/?ssl=true&ssl_ca_certs=rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false".format(credentials['UID'], credentials['PWD'],credentials['mydbcluster'])
    )
    if dbName is None:
        return client
    else:
        db = client[dbName]
        return db


def mongo_import(destination: str, env:str,myData: list, collection_to_use: str, db_to_use: str, meta=None):

    # if no 'meta' dict is passed, create one with just user and timestamp
    if meta is None:
        meta = {
            'timestamp': datetime.datetime.now(),
            'user': get_user()
        }

    # connect to specified database, if it doesnt exist, create it
    db = MongoDB_Client(destination=destination, env=env, dbName=db_to_use)

    # set collection equal to passed string, if it doesn't exist, MongoDB will automatically create it
    col = db[collection_to_use]

    # attach meta info to all items in list
    for i in myData:
        i['_meta'] = meta

    # count records pre-insert
    print("currently", col.count_documents({}), "in collection", col)

    # then insert into specified collection
    result = col.insert_many(myData)
    print(len(result.inserted_ids),"ids inserted")
    print("now", col.count_documents({}), "in collection", col)


def mongo_export_find(destination:str, env:str,collection_to_use: str, db_to_use: str, Filter=None, Project=None):

    User = get_user()

    if Project is None:
        Project = {}

    if Filter is None:
        Filter = {}

    # connect to specified database
    db = MongoDB_Client(destination=destination, env=env,dbName= db_to_use)

    # set collection equal to passed string
    col = db[collection_to_use]

    output = list(col.find(Filter,Project))

    return output

def mongo_export_pipeline(destination:str, env:str,collection_to_use: str, db_to_use: str, Pipeline: list):

    User = get_user()

    # connect to specified database
    db = MongoDB_Client(destination=destination, env=env, dbName = db_to_use)

    # set collection equal to passed string
    col = db[collection_to_use]

    output = list(col.aggregate(Pipeline))

    return output