import os
import json
import sqlalchemy
import urllib
from sqlalchemy import create_engine
from sqlalchemy.dialects import postgresql


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

def create_postgres_engine(destination: str, target_db: str, env: str):

    credentials = get_connection_strings(destination)
    env = env.upper()

    uid =urllib.parse.quote("{}".format(credentials[env]['UID']))
    pwd = urllib.parse.quote("{}".format(credentials[env]['PWD']))
    host=urllib.parse.quote("{}".format(credentials[env]['HOST']))
    port=urllib.parse.quote("{}".format(credentials[env]['PORT']))
    
    connection_string = "postgresql://{}:{}@{}:{}/{}".format(uid, pwd, host, port, target_db)

    engine = sqlalchemy.create_engine(connection_string)

    return engine

if __name__ == "__main__":
    engine=create_postgres_engine("AWS_PostGIS", "postgres", "DEV")
    query = 'select distinct TABLE_NAME from postgres.INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = \'public\''
    tables = [r.lower() for (r,) in list(engine.execute(query).fetchall())]
    for table in tables:
        print(table)
    test=engine.execute('SELECT aws_commons.create_s3_uri(\'dc-crash-bot-test\', \'census_block_level_final.csv\',\'us-east-1\') AS s3_uri').fetchall()
    print(test)