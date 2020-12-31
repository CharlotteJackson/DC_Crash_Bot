import sqlalchemy
from connect_to_rds import get_connection_strings, create_postgres_engine

dbname='postgres'
env="DEV"
engine = create_postgres_engine(destination="AWS_PostGIS", target_db=dbname, env=env)
db_credentials = get_connection_strings("AWS_PostGIS")

datasets_scripts = {
    'tmp_crashes_by_census_block':"""
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
    """
    ,'tmp_vz_by_census_block':"""
        SELECT 
            B.OBJECTID AS CENSUS_BLOCK_ID 
            ,COUNT(DISTINCT a.OBJECTID) AS TOTAL_VISION_ZERO_REQUESTS
            ,DATE_PART('year', cast(a.REQUESTDATE as TIMESTAMP)) AS YEAR
        FROM source_data.vision_zero a
        INNER JOIN source_data.census_blocks b ON ST_Intersects(a.geometry, b.geometry)
        GROUP BY b.OBJECTID, DATE_PART('year', cast(a.REQUESTDATE as TIMESTAMP))
    """
    ,'tmp_311_by_census_block':"""
        SELECT 
            B.OBJECTID AS CENSUS_BLOCK_ID 
            ,COUNT(DISTINCT a.OBJECTID) AS TOTAL_TSA_311_REQUESTS
            ,DATE_PART('year', cast(a.adddate as TIMESTAMP)) AS YEAR
        FROM source_data.all311 a
        INNER JOIN source_data.census_blocks b ON ST_Intersects(a.geometry, b.geometry)
        GROUP BY b.OBJECTID, DATE_PART('year', cast(a.adddate as TIMESTAMP))
    """
    ,'tmp_final_census_block_join':"""
        SELECT 
            crashes.*
            ,COALESCE(vz.TOTAL_VISION_ZERO_REQUESTS,0) AS TOTAL_VISION_ZERO_REQUESTS
            ,COALESCE(all311.TOTAL_TSA_311_REQUESTS,0) AS TOTAL_TSA_311_REQUESTS
            ,census_blocks.*
        FROM tmp.tmp_crashes_by_census_block crashes
        LEFT JOIN tmp.tmp_311_by_census_block all311 on all311.YEAR = crashes.YEAR and all311.CENSUS_BLOCK_ID = crashes.CENSUS_BLOCK_ID
        LEFT JOIN tmp.tmp_vz_by_census_block vz on vz.YEAR = crashes.YEAR and vz.CENSUS_BLOCK_ID = crashes.CENSUS_BLOCK_ID
        INNER JOIN source_data.census_blocks census_blocks ON census_blocks.OBJECTID = crashes.CENSUS_BLOCK_ID
        """
        
    ,'census_block_level_crashes':"""
        SELECT * FROM tmp.tmp_final_census_block_join
        """
}

analysis_tables = ['census_block_level_crashes']

for table in datasets_scripts.keys():

    if table in analysis_tables:
        schema='analysis_data'
    else:
        schema='tmp'

    create_schema_query="""
        CREATE SCHEMA IF NOT EXISTS {0};
        GRANT ALL PRIVILEGES ON SCHEMA {0} TO PUBLIC;
    """.format(schema)

    create_table_query ="""
    DROP TABLE IF EXISTS {0}.{1};
    CREATE TABLE {0}.{1} 
    AS (
        {2}
    );
    GRANT ALL PRIVILEGES ON {0}.{1} TO PUBLIC;
    """.format(schema, table, datasets_scripts[table])

    engine.execute(create_schema_query)
    engine.execute(create_table_query)

