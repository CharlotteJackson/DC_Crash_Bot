import sqlalchemy
from connect_to_rds import get_connection_strings, create_postgres_engine
import requests
import json 

dbname='postgres'
env="DEV"
engine = create_postgres_engine(destination="AWS_PostGIS", target_db=dbname, env=env)
db_credentials = get_connection_strings("AWS_PostGIS")
WALKSCORE_API_KEY = get_connection_strings("WALKSCORE")["API_Key"]
# walkscore-specific variables
walkscore_endpoint = 'https://api.walkscore.com/score'
format = 'json'

# First move all source data records to a temp table
step_1_query="""

CREATE TABLE IF NOT EXISTS tmp.address_walkscores as 
(
    SELECT * FROM 
    (  
        SELECT DISTINCT b.objectid as census_block_objectid, a.fulladdress, a.latitude, a.longitude, b.geography
            , ROW_NUMBER() over (PARTITION BY b.objectid ORDER BY a.fulladdress) AS row_num
        FROM source_data.address_points a
        INNER JOIN source_data.census_blocks b on ST_Intersects(a.geography::geometry, b.geography::geometry)
    ) as tmp WHERE ROW_NUM = 1
);

CREATE INDEX IF NOT EXISTS address_index ON tmp.address_walkscores (fulladdress);
CREATE INDEX IF NOT EXISTS census_block_index ON tmp.address_walkscores (census_block_objectid);
"""

engine.execute(step_1_query)
print("temp table created")

# create the geocodes table if they don't already exist
create_tables_query = """
CREATE TABLE IF NOT EXISTS source_data.address_walkscores (
    fulladdress varchar null
    ,census_block_objectid varchar null
    ,geography geography null
    ,latitude numeric null
    ,longitude numeric null
    ,walkscore numeric null
    ,bikescore numeric null
    ,transitscore numeric null
);

CREATE INDEX IF NOT EXISTS fulladdress_index ON source_data.address_walkscores (fulladdress);
CREATE INDEX IF NOT EXISTS census_block_index ON source_data.address_walkscores (census_block_objectid);

"""
engine.execute(create_tables_query)
print("geocode tables created")

# extract al the locations that need to be geocoded
get_addresses_to_check_query = """
select distinct a.fulladdress,a.census_block_objectid , a.latitude, a.longitude, a.geography
from tmp.address_walkscores a
left join source_data.address_walkscores b on a.fulladdress = b.fulladdress and a.census_block_objectid = b.census_block_objectid
where b.fulladdress is null
"""
records = engine.execute(get_addresses_to_check_query).fetchall()
print(len(records)," records without walkscore pulled for an update")

# then using the google maps API, add a lat and long for addresses that don't have them
for record in records:
    address = str(record[0])
    lat=record[2]
    long=record[3]
    census_block = record[1]
    geography = record[4]
    params = {"format": format, "address":address, "lat":lat, "lon":long, "bike":1, "transit":1
          ,"wsapikey":WALKSCORE_API_KEY}
    try:
        response = requests.get(walkscore_endpoint, params = params)
        content=json.loads(response.text)
        walkscore=content['walkscore']
        bikescore=content['bike']['score']
        transitscore=content['transit']['score']
        # insert into the table
        insert_record_query = f"INSERT INTO source_data.address_walkscores VALUES (\'{address}\',\'{census_block}\',{geography},{lat},{long},{walkscore},{bikescore},{transitscore})"
        engine.execute(insert_record_query)
    except Exception as error:
        print("could not find location for address ", address)
        continue

# check row counts
count_query = 'SELECT COUNT(*) FROM source_data.address_walkscores'
row_count = engine.execute(count_query).fetchone()[0]
print("query completed with ", row_count, " locations in walkscores table")
