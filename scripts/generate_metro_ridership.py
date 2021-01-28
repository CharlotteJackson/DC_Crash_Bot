import sqlalchemy
from connect_to_rds import get_connection_strings, create_postgres_engine
from add_location_info import add_location_info, add_school_info, create_final_table, add_roadway_info

dbname='postgres'
env="DEV"
engine = create_postgres_engine(destination="AWS_PostGIS", target_db=dbname, env=env)
db_credentials = get_connection_strings("AWS_PostGIS")

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

# First execute the table-specific queries
engine.execute(step1_query)
print("step1 query complete")

# Then execute the same location-info queries (roadway, schools, neighborhoods) that apply to all analysis tables and create the final table
next_tables = add_location_info(engine=engine, target_schema='tmp', target_table='metro_stations_ridership_nbh_ward', from_schema='tmp', from_table='metro_stations_ridership', partition_by_field='objectid')
print("neighborhood-ward query complete")
next_tables = add_school_info(engine=engine, target_schema='tmp', target_table='metro_stations_ridership_schools', from_schema=next_tables[0], from_table=next_tables[1])
print("schools query complete")
row_count = create_final_table(engine=engine, target_schema = 'analysis_data', target_table='metro_stations_ridership', from_schema=next_tables[0], from_table=next_tables[1])
print("final query complete with row count ",row_count)