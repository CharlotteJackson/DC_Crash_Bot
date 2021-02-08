from connect_to_rds import get_connection_strings, create_postgres_engine
from add_location_info import add_location_info, add_school_info, add_roadway_info, create_final_table

dbname='postgres'
env="DEV"
engine = create_postgres_engine(destination="AWS_PostGIS", target_db=dbname, env=env)
db_credentials = get_connection_strings("AWS_PostGIS")

step1_query ="""
DROP TABLE IF EXISTS tmp.intersection_points;
CREATE TABLE tmp.intersection_points
AS ( 
	SELECT 
        intersectionid,
         intersection_type
         , geography
         , ARRAY_AGG(DISTINCT fullstreet1display) as street_names
	FROM source_data.intersection_points
	GROUP BY intersectionid, intersection_type, geography
) ;

CREATE INDEX tmp_intersection_points_index ON tmp.intersection_points USING GIST (geography);

DROP TABLE IF EXISTS tmp.intersection_points_road_types;
CREATE TABLE tmp.intersection_points_road_types
AS ( 
	SELECT 
        a.*
        ,ARRAY_AGG(DISTINCT b.dcfunctionalclass_desc) as int_road_types
         , ARRAY_AGG(DISTINCT b.objectid) as int_road_block_ids
	FROM tmp.intersection_points a
    LEFT JOIN analysis_data.roadway_blocks b on ST_DWithin(a.geography, b.geography, 0.001)
	GROUP BY a.intersectionid, a.intersection_type, a.geography, a.street_names
) ;

CREATE INDEX tmp_intersection_points_road_types_index ON tmp.intersection_points_road_types USING GIST (geography);
"""

# First execute the table-specific queries
engine.execute(step1_query)
print("step1 query complete")

# Then execute the same location-info queries (roadway, schools, neighborhoods) that apply to all analysis tables and create the final table
next_tables = add_location_info(engine=engine, target_schema='tmp', target_table='intersection_points_nbh_ward', from_schema='tmp', from_table='intersection_points_road_types', partition_by_field='intersectionid')
print("neighborhood-ward query complete")
next_tables = add_school_info(engine=engine, target_schema='tmp', target_table='intersection_points_schools', from_schema=next_tables[0], from_table=next_tables[1])
print("schools query complete")
row_count = create_final_table(engine=engine, target_schema = 'analysis_data', target_table='intersection_points', from_schema=next_tables[0], from_table=next_tables[1])
print("final query complete with row count ",row_count)