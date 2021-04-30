from connect_to_rds import get_connection_strings, create_postgres_engine
from add_location_info import add_location_info,add_school_info,add_roadway_info, add_intersection_info,create_final_table,geocode_text

dbname='postgres'
env="DEV"
engine = create_postgres_engine(destination="AWS_PostGIS", env=env)
db_credentials = get_connection_strings("AWS_PostGIS")
# DROP TABLE IF EXISTS tmp.moving_violations_need_geo;
# DROP TABLE IF EXISTS tmp.moving_violations_has_geo;

# First move all source data records to a temp table
step_1_query="""

CREATE TABLE IF NOT EXISTS tmp.moving_violations_need_geo as 
SELECT * FROM source_data.moving_violations
WHERE geography IS NULL;

CREATE TABLE IF NOT EXISTS tmp.moving_violations_has_geo as 
SELECT * FROM source_data.moving_violations
WHERE geography IS NOT NULL;

CREATE INDEX IF NOT EXISTS mv_location_index ON tmp.moving_violations_need_geo (location);
"""

engine.execute(step_1_query)
print("temp table created")

# geocode the locations
records = [loc for (loc,) in engine.execute("select distinct location from tmp.moving_violations_need_geo where geography is null limit 2000").fetchall()]
print(len(records)," records passed to geocode function")
geocode_text(engine=engine, records_to_geocode = records, administrative_area='District of Columbia', text_type = 'Moving Violations location')

# update lat and long values from new data
step_2_query = """
UPDATE tmp.moving_violations_need_geo
   SET geography = source_data.geocoded_text.point_geography
   FROM source_data.geocoded_text 
   WHERE source_data.geocoded_text.text = location
   ;

INSERT INTO tmp.moving_violations_has_geo
SELECT * FROM tmp.moving_violations_need_geo;

CREATE INDEX IF NOT EXISTS mv_geom_idx ON tmp.moving_violations_has_geo USING GIST (geography);
"""

engine.execute(step_2_query)
print("geo values updated")

# Then execute the same location-info queries (roadway, schools, neighborhoods) that apply to all analysis tables and create the final table
next_tables = add_location_info(engine=engine, target_schema='tmp', target_table='moving_violations_nbh_ward', from_schema='tmp', from_table='moving_violations_has_geo', partition_by_field='objectid')
print("neighborhood-ward query complete")
next_tables = add_school_info(engine=engine, target_schema='tmp', target_table='moving_violations_schools', from_schema=next_tables[0], from_table=next_tables[1])
print("schools query complete")
next_tables = add_roadway_info(engine=engine, target_schema='tmp', target_table='moving_violations_roadway_info', from_schema=next_tables[0], from_table=next_tables[1], partition_by_field='objectid', within_distance= 50)
print("roadway info query complete")
next_tables = add_intersection_info(engine=engine, target_schema='tmp', target_table='moving_violations_intersection_info', from_schema=next_tables[0], from_table=next_tables[1], partition_by_field='objectid', within_distance= 20)
print("intersection info query complete")
row_count = create_final_table(engine=engine, target_schema = 'analysis_data', target_table='moving_violations', from_schema=next_tables[0], from_table=next_tables[1])
print("final query complete with row count ",row_count)