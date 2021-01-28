from connect_to_rds import get_connection_strings, create_postgres_engine
from add_location_info import add_location_info, add_school_info, add_roadway_info, create_final_table

dbname='postgres'
env="DEV"
engine = create_postgres_engine(destination="AWS_PostGIS", target_db=dbname, env=env)
db_credentials = get_connection_strings("AWS_PostGIS")

step1_query ="""
DROP TABLE IF EXISTS tmp.roadway_blocks;
CREATE TABLE tmp.roadway_blocks
AS ( 
SELECT *
    ,case when sidewalk_ib_pavtype is not null and sidewalk_ob_pavtype is not null then 2
            when sidewalk_ib_pavtype is not null or sidewalk_ob_pavtype is not null then 1
            else 0 end as Num_Sides_W_Sidewalks
    ,coalesce(sidewalk_ib_width, sidewalk_ob_width) as sidewalk_width
    ,coalesce(speedlimits_ib, speedlimits_ob) as speed_limit
    ,case dcfunctionalclass
        when '11.0' then 'Interstate'
        when '12.0' then 'Other Freeway and Expressway'
        when '14.0' then 'Principal Arterial'
        when '16.0' then 'Minor Arterial'
        when '17.0' then 'Collector'
        when '19.0' then 'Local'
        end as dcfunctionalclass_desc
FROM source_data.roadway_blocks 

);

CREATE INDEX tmp_roadway_blocks_index ON tmp.roadway_blocks USING GIST (geography);
"""

# First execute the table-specific queries
engine.execute(step1_query)
print("step1 query complete")

# Then execute the same location-info queries (roadway, schools, neighborhoods) that apply to all analysis tables and create the final table
next_tables = add_location_info(engine=engine, target_schema='tmp', target_table='roadway_blocks_nbh_ward', from_schema='tmp', from_table='roadway_blocks', partition_by_field='objectid')
print("neighborhood-ward query complete")
next_tables = add_school_info(engine=engine, target_schema='tmp', target_table='roadway_blocks_schools', from_schema=next_tables[0], from_table=next_tables[1])
print("schools query complete")
row_count = create_final_table(engine=engine, target_schema = 'analysis_data', target_table='roadway_blocks', from_schema=next_tables[0], from_table=next_tables[1])
print("final query complete with row count ",row_count)