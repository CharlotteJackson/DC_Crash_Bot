import sqlalchemy
from connect_to_rds import get_connection_strings, create_postgres_engine

dbname='postgres'
env="DEV"
engine = create_postgres_engine(destination="AWS_PostGIS", target_db=dbname, env=env)
db_credentials = get_connection_strings("AWS_PostGIS")

target_schema = 'analysis_data'
target_table='census_block_level_crashes'


roll_up_crashes_by_census_block_query ="""
CREATE TEMP TABLE tmp_crashes_by_census_block ON COMMIT PRESERVE ROWS 
AS (
    SELECT 
        B.OBJECTID AS CENSUS_BLOCK_ID 
        ,COUNT(DISTINCT a.CRIMEID) AS NUM_CRASHES
        ,EXTRACT (YEAR FROM a.FROMDATE) AS YEAR
        ,SUM(DRIVERS_OVER_80) AS DRIVERS_OVER_80
        ,SUM(DRIVERS_UNDER_25) AS DRIVERS_UNDER_25
        ,SUM(PEDS_OVER_70) AS PEDS_OVER_70
        ,SUM(PEDS_UNDER_12) AS PEDS_UNDER_12
        ,SUM(BIKERS_OVER_70) AS BIKERS_OVER_70
        ,SUM(OOS_VEHICLES) AS OOS_VEHICLES
        ,SUM(NUM_CARS) AS NUM_CARS
        ,SUM(NUM_SUVS_OR_TRUCKS) AS NUM_SUVS_OR_TRUCKS
        ,SUM(PEDESTRIAN_INJURIES) AS PEDESTRIAN_INJURIES
        ,SUM(BICYCLE_INJURIES) AS BICYCLE_INJURIES
        ,SUM(VEHICLE_INJURIES) AS VEHICLE_INJURIES
        ,SUM(PEDESTRIAN_FATALITIES) AS PEDESTRIAN_FATALITIES
        ,SUM(BICYCLE_FATALITIES) AS BICYCLE_FATALITIES
        ,SUM(VEHICLE_FATALITIES) AS VEHICLE_FATALITIES
        ,SUM(DRIVER_TICKETS) AS DRIVER_TICKETS
        ,SUM(BICYCLE_TICKETS) AS BICYCLE_TICKETS
        ,SUM(PED_TICKETS) AS PED_TICKETS
        ,SUM(TOTAL_INJURIES) AS TOTAL_INJURIES
        ,SUM(TOTAL_VEHICLES) AS TOTAL_VEHICLES
        ,SUM(TOTAL_PEDESTRIANS) AS TOTAL_PEDESTRIANS
        ,SUM(TOTAL_BICYCLISTS) AS TOTAL_BICYCLISTS
    FROM analysis_data.dc_crashes_w_details a
    INNER JOIN source_data.census_blocks b ON ST_Intersects(a.geometry, b.geometry)
    GROUP BY b.OBJECTID, EXTRACT (YEAR FROM a.FROMDATE)
) WITH DATA;
"""

roll_up_vz_by_census_block_query ="""
CREATE TEMP TABLE tmp_vz_by_census_block ON COMMIT PRESERVE ROWS 
AS (
    SELECT 
        B.OBJECTID AS CENSUS_BLOCK_ID 
        ,COUNT(DISTINCT a.OBJECTID) AS TOTAL_VISION_ZERO_REQUESTS
        ,EXTRACT (YEAR FROM a.REQUESTDATE) AS YEAR
    FROM source_data.vision_zero a
    INNER JOIN source_data.census_blocks b ON ST_Intersects(a.geometry, b.geometry)
    GROUP BY b.OBJECTID, EXTRACT (YEAR FROM a.REQUESTDATE) 
) WITH DATA;
"""

roll_up_311_by_census_block_query ="""
CREATE TEMP TABLE tmp_311_by_census_block ON COMMIT PRESERVE ROWS 
AS (
    SELECT 
        B.OBJECTID AS CENSUS_BLOCK_ID 
        ,COUNT(DISTINCT a.OBJECTID) AS TOTAL_TSA_311_REQUESTS
        ,EXTRACT (YEAR FROM a.REQUESTDATE) AS YEAR
    FROM source_data.all311 a
    INNER JOIN source_data.census_blocks b ON ST_Intersects(a.geometry, b.geometry)
    GROUP BY b.OBJECTID, EXTRACT (YEAR FROM a.REQUESTDATE) 
) WITH DATA;
"""

join_query="""
CREATE TEMP TABLE tmp_final_census_block_join ON COMMIT PRESERVE ROWS 
AS (
    SELECT 
        crashes.*
        ,vz.TOTAL_VISION_ZERO_REQUESTS
        ,all311.TOTAL_TSA_311_REQUESTS
        ,census_blocks.*
    FROM tmp_crashes_by_census_block crashes
    INNER JOIN tmp_311_by_census_block all311 on all311.YEAR = crashes.YEAR and all311.CENSUS_BLOCK_ID = crashes.CENSUS_BLOCK_ID
    INNER JOIN tmp_vz_by_census_block vz on vz.YEAR = crashes.YEAR and vz.CENSUS_BLOCK_ID = crashes.CENSUS_BLOCK_ID
    INNER JOIN source_data.census_blocks census_blocks ON census_blocks.OBJECTID = crashes.CENSUS_BLOCK_ID
    
) WITH DATA;
"""

final_query="""
DROP TABLE IF EXISTS {0}.{1};

CREATE TABLE {0}.{1} AS 
    SELECT * FROM tmp_final_census_block_join;
    
GRANT ALL PRIVILEGES ON {0}.{1} TO PUBLIC;
""".format(target_schema, target_table)

engine.execute(roll_up_crashes_by_census_block_query)
engine.execute(roll_up_vz_by_census_block_query)
engine.execute(roll_up_311_by_census_block_query)
engine.execute(join_query)
engine.execute(final_query)        