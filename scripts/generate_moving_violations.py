import sqlalchemy
from connect_to_rds import get_connection_strings, create_postgres_engine
from add_location_info import add_location_info,add_school_info,add_roadway_info, add_intersection_info,create_final_table
from get_address import GeoLoc
import urllib 

dbname='postgres'
env="DEV"
engine = create_postgres_engine(destination="AWS_PostGIS", target_db=dbname, env=env)
db_credentials = get_connection_strings("AWS_PostGIS")
GOOGLE_API_KEY = get_connection_strings("GOOGLE_MAPS")["API_Key"]

# First move all source data records to a temp table
step_1_query="""
DROP TABLE IF EXISTS tmp.moving_violations;

CREATE TABLE tmp.moving_violations as 
SELECT * FROM source_data.moving_violations;

"""

engine.execute(step_1_query)
print("temp table created")

# create the geocodes table if they don't already exist
create_tables_query = """
CREATE TABLE IF NOT EXISTS source_data.mv_geocodes (
    objectid varchar null
    ,location varchar null
    ,lat numeric null
    ,long numeric null
);

CREATE TABLE IF NOT EXISTS source_data.mv_location_no_geocodes (
    objectid varchar null
    ,location varchar null
);
"""
engine.execute(create_tables_query)
print("geocode tables created")

# extract al the locations that need to be geocoded
get_locations_to_geocode_query = """
select a.objectid, a.location 
from tmp.moving_violations a
left join source_data.mv_geocodes b on a.objectid = b.objectid
left join source_data.mv_location_no_geocodes c on a.objectid = c.objectid
where a.longitude is null and a.latitude is null and b.objectid is null and c.objectid is null
"""
records = [[id, loc] for (id,loc) in engine.execute(get_locations_to_geocode_query).fetchall()]
print(len(records)," records without location pulled for an update")

# then using the google maps API, add a lat and long for addresses that don't have them
for record in records:
    address = str(record[1])
    objectid = record[0]
    address_url_enc = urllib.parse.quote(address)
    try:
        geo_loc_instance = GeoLoc(GOOGLE_API_KEY)
        lat_long = geo_loc_instance.GetGeoLoc(address_url_enc)
        # insert into the table
        insert_record_query = "INSERT INTO source_data.mv_geocodes VALUES (\'{0}\',\'{1}\',{2},{3})".format(objectid, address, lat_long["lat"], lat_long["lng"])
        engine.execute(insert_record_query)
    except Exception as error:
        print("could not find location for address ", address)
        # insert into the missing values table
        insert_missing_record_query = "INSERT INTO source_data.mv_location_no_geocodes VALUES (\'{0}\',\'{1}\')".format(objectid, address)
        engine.execute(insert_missing_record_query)
        continue

# check row counts
count_query = 'SELECT COUNT(*) FROM source_data.mv_geocodes'
row_count = engine.execute(count_query).fetchone()[0]
print("query compelted with ", row_count, " objectid's in geocodes table")

# update lat and long values from new data
step_2_query = """
UPDATE tmp.moving_violations
   SET latitude = source_data.mv_geocodes.lat , longitude = source_data.mv_geocodes.long 
   FROM tmp.moving_violations a INNER JOIN  source_data.mv_geocodes ON tmp.mv_geocodes.objectid = a.objectid
   WHERE a.latitude IS NULL and a.longitude IS NULL
"""

engine.execute(step_2_query)
print("lat and long values updated")

# create geography field with populated lat and long values, and create index
step_3_query = """
UPDATE tmp.moving_violations
SET geography = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography
WHERE geography IS NULL AND longitude is not null AND latitude is not null
;

CREATE INDEX IF NOT EXISTS mv_geom_idx ON tmp.moving_violations USING GIST (geography);
"""
engine.execute(step_2_query)
print("geography field created")

# Then execute the same location-info queries (roadway, schools, neighborhoods) that apply to all analysis tables and create the final table
next_tables = add_location_info(engine=engine, target_schema='tmp', target_table='moving_violations_nbh_ward', from_schema='tmp', from_table='moving_violations', partition_by_field='objectid')
print("neighborhood-ward query complete")
next_tables = add_school_info(engine=engine, target_schema='tmp', target_table='moving_violations_schools', from_schema=next_tables[0], from_table=next_tables[1])
print("schools query complete")
next_tables = add_roadway_info(engine=engine, target_schema='tmp', target_table='moving_violations_roadway_info', from_schema=next_tables[0], from_table=next_tables[1], partition_by_field='objectid', within_distance= 10)
print("roadway info query complete")
next_tables = add_intersection_info(engine=engine, target_schema='tmp', target_table='moving_violations_intersection_info', from_schema=next_tables[0], from_table=next_tables[1], partition_by_field='objectid', within_distance= 20)
print("intersection info query complete")
row_count = create_final_table(engine=engine, target_schema = 'analysis_data', target_table='moving_violations', from_schema=next_tables[0], from_table=next_tables[1])
print("final query complete with row count ",row_count)