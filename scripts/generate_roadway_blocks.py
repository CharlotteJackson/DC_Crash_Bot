from connect_to_rds import get_connection_strings, create_postgres_engine
from add_location_info import add_location_info
from add_school_info import add_school_info

dbname='postgres'
env="DEV"
engine = create_postgres_engine(destination="AWS_PostGIS", target_db=dbname, env=env)
db_credentials = get_connection_strings("AWS_PostGIS")

target_schema = 'analysis_data'
target_table='roadway_blocks'

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

final_query="""
DROP TABLE IF EXISTS {0}.{1};

CREATE TABLE {0}.{1} AS 
    SELECT * FROM tmp.roadway_blocks_schools;

GRANT ALL PRIVILEGES ON {0}.{1} TO PUBLIC;

CREATE INDEX {0}_{1}_index ON {0}.{1} USING GIST (geography);
""".format(target_schema, target_table)

engine.execute(step1_query)
print("initial query complete")
# add all the boundaries
add_location_info(engine=engine, target_schema='tmp', target_table='roadway_blocks_nbh_ward', from_schema='tmp', from_table='roadway_blocks', partition_by_field='objectid')
print("neighborhood-ward query complete")
# add nearby schools
add_school_info(engine=engine, target_schema='tmp', target_table='roadway_blocks_schools', from_schema='tmp', from_table='roadway_blocks_nbh_ward')
print("schools query complete")
engine.execute(final_query)
print("final query complete")