import sqlalchemy
from connect_to_rds import get_connection_strings, create_postgres_engine
from add_location_info import add_location_info

dbname='postgres'
env="DEV"
engine = create_postgres_engine(destination="AWS_PostGIS", target_db=dbname, env=env)
db_credentials = get_connection_strings("AWS_PostGIS")

target_schema = 'analysis_data'
target_table='metro_stations_ridership'

step1_query ="""
DROP TABLE IF EXISTS tmp.metro_stations_ridership;
CREATE TABLE tmp.metro_stations_ridership
AS ( 
SELECT 
    objectid
    ,station
    ,stationnumber
    ,y2015
    ,y2016
    ,y2017
    ,y2018
    ,(y2015+y2016+y2017+y2018)/4.00 as avg_2015_2018
    ,cast(ycoord as numeric) as latitude
    ,cast(xcoord as numeric) as longitude
    ,ST_SetSRID(ST_MakePoint(cast(xcoord as numeric), cast(ycoord as numeric)), 4326)::geography as geography
FROM source_data.metro_stations_daily_ridership 
WHERE station is not null
)
"""

final_query="""
DROP TABLE IF EXISTS {0}.{1};

CREATE TABLE {0}.{1} AS 
    SELECT * FROM tmp.metro_stations_nbh_ward;

GRANT ALL PRIVILEGES ON {0}.{1} TO PUBLIC;

CREATE INDEX {0}_{1}_index ON {0}.{1} USING GIST (geography);
""".format(target_schema, target_table)

engine.execute(step1_query)
print("initial query complete")
# add all the boundaries
add_location_info(engine=engine, target_schema='tmp', target_table='metro_stations_nbh_ward', from_schema='tmp', from_table='metro_stations_ridership', partition_by_field='objectid')
print("neighborhood-ward query complete")
engine.execute(final_query)