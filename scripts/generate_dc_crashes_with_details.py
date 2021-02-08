import sqlalchemy
from connect_to_rds import get_connection_strings, create_postgres_engine
from add_location_info import add_location_info, add_school_info, add_roadway_info, add_intersection_info, create_final_table

# create database connection
dbname='postgres'
env="DEV"
engine = create_postgres_engine(destination="AWS_PostGIS", target_db=dbname, env=env)
db_credentials = get_connection_strings("AWS_PostGIS")


# The queries that are specific to the crash data and are not run anywhere else
add_columns_query ="""
DROP TABLE IF EXISTS tmp.crash_details;
CREATE TABLE tmp.crash_details 
AS (
    SELECT *
        ,CASE WHEN PERSONTYPE = 'Driver' AND AGE >=65 THEN 1 ELSE 0 END AS DRIVERS_OVER_65
        ,CASE WHEN PERSONTYPE = 'Driver' AND AGE <=25 THEN 1 ELSE 0 END AS DRIVERS_UNDER_25
        ,CASE WHEN PERSONTYPE = 'Pedestrian' AND AGE >=65 THEN 1 ELSE 0 END AS PEDS_OVER_65
        ,CASE WHEN PERSONTYPE = 'Pedestrian' AND AGE <=12 THEN 1 ELSE 0 END AS PEDS_UNDER_12
        ,CASE WHEN PERSONTYPE = 'Bicyclist' AND AGE >=65 THEN 1 ELSE 0 END AS BIKERS_OVER_65
        ,CASE WHEN PERSONTYPE = 'Bicyclist' AND AGE <=18 THEN 1 ELSE 0 END AS BIKERS_UNDER_18
        ,CASE WHEN PERSONTYPE = 'Driver' AND LICENSEPLATESTATE <> 'DC' AND LICENSEPLATESTATE <> ' None' THEN 1 ELSE 0 END AS OOS_VEHICLES
        ,CASE WHEN PERSONTYPE = 'Driver' AND INVEHICLETYPE = 'Passenger Car/automobile' THEN 1 ELSE 0 END AS NUM_CARS
        ,CASE WHEN PERSONTYPE = 'Driver' AND INVEHICLETYPE in ('Suv (sport Utility Vehicle)', 'Pickup Truck') THEN 1 ELSE 0 END AS NUM_SUVS_OR_TRUCKS

        ,CASE WHEN PERSONTYPE = 'Pedestrian' AND FATAL='Y' THEN 1 ELSE 0 END AS PED_FATALITIES
        ,CASE WHEN PERSONTYPE = 'Bicyclist' AND FATAL='Y'THEN 1 ELSE 0 END AS BICYCLE_FATALITIES
        ,CASE WHEN PERSONTYPE in ('Driver','Passenger') AND FATAL='Y' THEN 1 ELSE 0 END AS VEHICLE_FATALITIES

        ,CASE WHEN PERSONTYPE = 'Pedestrian' AND (MAJORINJURY='Y' OR MINORINJURY ='Y')THEN 1 ELSE 0 END AS PED_INJURIES
        ,CASE WHEN PERSONTYPE = 'Bicyclist' AND (MAJORINJURY='Y' OR MINORINJURY ='Y') THEN 1 ELSE 0 END AS BICYCLE_INJURIES
        ,CASE WHEN PERSONTYPE in ('Driver','Passenger') AND (MAJORINJURY='Y' OR MINORINJURY ='Y') THEN 1 ELSE 0 END AS VEHICLE_INJURIES
        ,CASE WHEN PERSONTYPE = 'Driver' AND TICKETISSUED ='Y' THEN 1 ELSE 0 END AS DRIVER_TICKETS
        ,CASE WHEN PERSONTYPE = 'Driver' AND SPEEDING ='Y' THEN 1 ELSE 0 END AS DRIVERS_SPEEDING
        ,CASE WHEN PERSONTYPE = 'Driver' AND IMPAIRED ='Y' THEN 1 ELSE 0 END AS DRIVERS_IMPAIRED

        ,CASE WHEN PERSONTYPE = 'Bicyclist' AND TICKETISSUED ='Y' THEN 1 ELSE 0 END AS BICYCLE_TICKETS
        ,CASE WHEN PERSONTYPE = 'Pedestrian' AND TICKETISSUED ='Y'  THEN 1 ELSE 0 END AS PED_TICKETS
        ,CASE WHEN (MAJORINJURY='Y' OR MINORINJURY ='Y') THEN 1 ELSE 0 END AS TOTAL_INJURIES
        ,CASE WHEN MAJORINJURY='Y' THEN 1 ELSE 0 END AS TOTAL_MAJOR_INJURIES
        ,CASE WHEN MINORINJURY ='Y' THEN 1 ELSE 0 END AS TOTAL_MINOR_INJURIES

        ,CASE WHEN PERSONTYPE = 'Driver' THEN 1 ELSE 0 END AS TOTAL_VEHICLES
        ,CASE WHEN PERSONTYPE = 'Pedestrian' THEN 1 ELSE 0 END AS TOTAL_PEDESTRIANS
        ,CASE WHEN PERSONTYPE = 'Bicyclist' THEN 1 ELSE 0 END AS TOTAL_BICYCLISTS
    FROM source_data.crash_details
)
"""
group_by_query = """
DROP TABLE IF EXISTS tmp.crash_details_agg;
CREATE  TABLE tmp.crash_details_agg 
AS (
    SELECT 
        CRIMEID
        ,SUM(DRIVERS_OVER_65) AS DRIVERS_OVER_65
        ,SUM(DRIVERS_UNDER_25) AS DRIVERS_UNDER_25
        ,SUM(PEDS_OVER_65) AS PEDS_OVER_65
        ,SUM(PEDS_UNDER_12) AS PEDS_UNDER_12
        ,SUM(BIKERS_OVER_65) AS BIKERS_OVER_65
        ,SUM(BIKERS_UNDER_18) AS BIKERS_UNDER_18
        ,SUM(OOS_VEHICLES) AS OOS_VEHICLES
        ,SUM(NUM_CARS) AS NUM_CARS
        ,SUM(NUM_SUVS_OR_TRUCKS) AS NUM_SUVS_OR_TRUCKS
        ,SUM(PED_INJURIES) AS PEDESTRIAN_INJURIES
        ,SUM(BICYCLE_INJURIES) AS BICYCLE_INJURIES
        ,SUM(VEHICLE_INJURIES) AS VEHICLE_INJURIES
        ,SUM(PED_FATALITIES) AS PEDESTRIAN_FATALITIES
        ,SUM(BICYCLE_FATALITIES) AS BICYCLE_FATALITIES
        ,SUM(VEHICLE_FATALITIES) AS VEHICLE_FATALITIES
        ,SUM(DRIVER_TICKETS) AS DRIVER_TICKETS
        ,SUM(DRIVERS_SPEEDING) AS DRIVERS_SPEEDING
        ,SUM(DRIVERS_IMPAIRED) AS DRIVERS_IMPAIRED
        ,SUM(BICYCLE_TICKETS) AS BICYCLE_TICKETS
        ,SUM(PED_TICKETS) AS PED_TICKETS
        ,SUM(TOTAL_INJURIES) AS TOTAL_INJURIES
        ,SUM(TOTAL_MAJOR_INJURIES) AS TOTAL_MAJOR_INJURIES
        ,SUM(TOTAL_MINOR_INJURIES) AS TOTAL_MINOR_INJURIES
        ,SUM(TOTAL_VEHICLES) AS TOTAL_VEHICLES
        ,SUM(TOTAL_PEDESTRIANS) AS TOTAL_PEDESTRIANS
        ,SUM(TOTAL_BICYCLISTS) AS TOTAL_BICYCLISTS
        ,ARRAY_AGG(PERSONTYPE) AS PERSONTYPE_ARRAY
        ,ARRAY_AGG(INVEHICLETYPE) AS INVEHICLETYPE_ARRAY
        ,ARRAY_AGG(LICENSEPLATESTATE) AS LICENSEPLATESTATE_ARRAY
    FROM tmp.crash_details
    GROUP BY CRIMEID
) ;
create index crime_id on tmp.crash_details_agg (crimeid);
"""

join_query = """
DROP TABLE IF EXISTS tmp.crashes_join;
CREATE TABLE tmp.crashes_join
AS (
    SELECT 
        a.OBJECTID
            ,a.CRIMEID
            ,a.REPORTDATE
            ,a.FROMDATE
            ,a.TODATE 
            ,a.ADDRESS
            ,a.latitude
            ,a.longitude
            ,CASE WHEN b.CRIMEID IS NULL OR b.BICYCLE_INJURIES < (a.MAJORINJURIES_BICYCLIST + a.MINORINJURIES_BICYCLIST + a.UNKNOWNINJURIES_BICYCLIST)
                THEN (a.MAJORINJURIES_BICYCLIST + a.MINORINJURIES_BICYCLIST + a.UNKNOWNINJURIES_BICYCLIST)
                ELSE b.BICYCLE_INJURIES END AS BICYCLE_INJURIES
            ,CASE WHEN b.CRIMEID IS NULL OR b.VEHICLE_INJURIES < (a.MAJORINJURIES_DRIVER+a.MINORINJURIES_DRIVER+a.UNKNOWNINJURIES_DRIVER+a.MAJORINJURIESPASSENGER+a.MINORINJURIESPASSENGER+a.UNKNOWNINJURIESPASSENGER)
                THEN (a.MAJORINJURIES_DRIVER+a.MINORINJURIES_DRIVER+a.UNKNOWNINJURIES_DRIVER+a.MAJORINJURIESPASSENGER+a.MINORINJURIESPASSENGER+a.UNKNOWNINJURIESPASSENGER)
                ELSE b.VEHICLE_INJURIES END AS VEHICLE_INJURIES
            ,CASE WHEN b.CRIMEID IS NULL OR b.PEDESTRIAN_INJURIES < (a.MAJORINJURIES_PEDESTRIAN+ a.MINORINJURIES_PEDESTRIAN + a.UNKNOWNINJURIES_PEDESTRIAN)
                THEN (a.MAJORINJURIES_PEDESTRIAN + a.MINORINJURIES_PEDESTRIAN + a.UNKNOWNINJURIES_PEDESTRIAN)
                ELSE b.PEDESTRIAN_INJURIES END AS PEDESTRIAN_INJURIES
            ,CASE WHEN b.CRIMEID IS NULL OR b.TOTAL_INJURIES < (a.MAJORINJURIES_PEDESTRIAN+ a.MINORINJURIES_PEDESTRIAN + a.UNKNOWNINJURIES_PEDESTRIAN
                                                                +a.MAJORINJURIES_DRIVER+a.MINORINJURIES_DRIVER+a.UNKNOWNINJURIES_DRIVER+a.MAJORINJURIESPASSENGER+a.MINORINJURIESPASSENGER+a.UNKNOWNINJURIESPASSENGER
                                                                +a.MAJORINJURIES_BICYCLIST + a.MINORINJURIES_BICYCLIST + a.UNKNOWNINJURIES_BICYCLIST)
                    THEN (a.MAJORINJURIES_PEDESTRIAN+ a.MINORINJURIES_PEDESTRIAN + a.UNKNOWNINJURIES_PEDESTRIAN
                                                                +a.MAJORINJURIES_DRIVER+a.MINORINJURIES_DRIVER+a.UNKNOWNINJURIES_DRIVER+a.MAJORINJURIESPASSENGER+a.MINORINJURIESPASSENGER+a.UNKNOWNINJURIESPASSENGER
                                                                +a.MAJORINJURIES_BICYCLIST + a.MINORINJURIES_BICYCLIST + a.UNKNOWNINJURIES_BICYCLIST)
                    ELSE b.TOTAL_INJURIES end as TOTAL_INJURIES 

            ,CASE WHEN b.CRIMEID IS NULL OR b.TOTAL_MAJOR_INJURIES < (a.MAJORINJURIES_PEDESTRIAN+
                                                                +a.MAJORINJURIES_DRIVER+a.MAJORINJURIESPASSENGER
                                                                +a.MAJORINJURIES_BICYCLIST)
                    THEN (a.MAJORINJURIES_PEDESTRIAN+a.MAJORINJURIES_DRIVER+a.MAJORINJURIESPASSENGER+a.MAJORINJURIES_BICYCLIST)
                    ELSE b.TOTAL_MAJOR_INJURIES end as TOTAL_MAJOR_INJURIES 

            ,CASE WHEN b.CRIMEID IS NULL OR b.TOTAL_MINOR_INJURIES < (a.MINORINJURIES_PEDESTRIAN + a.UNKNOWNINJURIES_PEDESTRIAN
                                                                +a.MINORINJURIES_DRIVER+a.UNKNOWNINJURIES_DRIVER+a.MINORINJURIESPASSENGER+a.UNKNOWNINJURIESPASSENGER
                                                                +a.MINORINJURIES_BICYCLIST + a.UNKNOWNINJURIES_BICYCLIST)
                   THEN (a.MINORINJURIES_PEDESTRIAN + a.UNKNOWNINJURIES_PEDESTRIAN
                                                                +a.MINORINJURIES_DRIVER+a.UNKNOWNINJURIES_DRIVER+a.MINORINJURIESPASSENGER+a.UNKNOWNINJURIESPASSENGER
                                                                +a.MINORINJURIES_BICYCLIST + a.UNKNOWNINJURIES_BICYCLIST)
                ELSE b.TOTAL_MINOR_INJURIES end as TOTAL_MINOR_INJURIES     

            ,CASE WHEN b.CRIMEID IS NULL OR b.BICYCLE_FATALITIES < a.FATAL_BICYCLIST
                THEN a.FATAL_BICYCLIST 
                ELSE b.BICYCLE_FATALITIES END AS BICYCLE_FATALITIES
            ,CASE WHEN b.CRIMEID IS NULL OR b.PEDESTRIAN_FATALITIES < a.FATAL_PEDESTRIAN
                THEN a.FATAL_PEDESTRIAN 
                ELSE b.PEDESTRIAN_FATALITIES END AS PEDESTRIAN_FATALITIES
            ,CASE WHEN b.CRIMEID IS NULL OR b.VEHICLE_FATALITIES < (a.FATAL_DRIVER+a.FATALPASSENGER)
                THEN (a.FATAL_DRIVER+a.FATALPASSENGER) 
                ELSE b.VEHICLE_FATALITIES END AS VEHICLE_FATALITIES
            ,CASE WHEN b.CRIMEID IS NULL or b.DRIVERS_IMPAIRED < a.DRIVERSIMPAIRED THEN a.DRIVERSIMPAIRED ELSE b.DRIVERS_IMPAIRED END AS DRIVERS_IMPAIRED 
            ,CASE WHEN b.CRIMEID IS NULL or b.DRIVERS_SPEEDING < a.SPEEDING_INVOLVED THEN a.SPEEDING_INVOLVED ELSE b.DRIVERS_SPEEDING END AS DRIVERS_SPEEDING 

            ,CASE WHEN b.CRIMEID IS NULL or b.TOTAL_VEHICLES < a.TOTAL_VEHICLES THEN a.TOTAL_VEHICLES ELSE b.TOTAL_VEHICLES END AS TOTAL_VEHICLES 
            ,CASE WHEN b.CRIMEID IS NULL or b.TOTAL_BICYCLISTS < a.TOTAL_BICYCLES THEN a.TOTAL_BICYCLES ELSE b.TOTAL_BICYCLISTS END AS TOTAL_BICYCLISTS 
            ,CASE WHEN b.CRIMEID IS NULL or b.TOTAL_PEDESTRIANS < a.TOTAL_PEDESTRIANS THEN a.TOTAL_PEDESTRIANS ELSE b.TOTAL_PEDESTRIANS END AS TOTAL_PEDESTRIANS 
            ,b.DRIVERS_OVER_65
            ,b.DRIVERS_UNDER_25
            ,b.PEDS_OVER_65
            ,b.PEDS_UNDER_12
            ,b.BIKERS_OVER_65
            ,b.BIKERS_UNDER_18
            ,b.OOS_VEHICLES
            ,b.NUM_CARS
            ,b.NUM_SUVS_OR_TRUCKS
            ,b.DRIVER_TICKETS
            ,b.BICYCLE_TICKETS
            ,b.PED_TICKETS
            ,b.PERSONTYPE_ARRAY
            ,b.INVEHICLETYPE_ARRAY
            ,b.LICENSEPLATESTATE_ARRAY
            ,a.INTAPPROACHDIRECTION
            ,a.LOCATIONERROR 
            ,a.LASTUPDATEDATE
            ,a.BLOCKKEY
            ,a.SUBBLOCKKEY
            ,ST_Force2D(a.geography::geometry) as geography

    FROM source_data.crashes_raw a
    LEFT JOIN tmp.crash_details_agg b on a.CRIMEID = b.CRIMEID
) ;
CREATE INDEX crashes_geom_idx ON tmp.crashes_join USING GIST (geography);
"""

# First execute the table-specific queries
engine.execute(add_columns_query)
print("add columns query complete")
engine.execute(group_by_query)
print("group by query complete")
engine.execute(join_query)
print("join query complete")

# Then execute the same location-info queries (roadway, schools, neighborhoods) that apply to all analysis tables and create the final table
next_tables = add_location_info(engine=engine, target_schema='tmp', target_table='crashes_nbh_ward', from_schema='tmp', from_table='crashes_join', partition_by_field='objectid')
print("neighborhood-ward query complete")
next_tables = add_school_info(engine=engine, target_schema='tmp', target_table='crashes_schools', from_schema=next_tables[0], from_table=next_tables[1])
print("schools query complete")
next_tables = add_roadway_info(engine=engine, target_schema='tmp', target_table='crashes_roadway_info', from_schema=next_tables[0], from_table=next_tables[1], partition_by_field='objectid', within_distance= 0.001)
print("roadway info query complete")
next_tables = add_intersection_info(engine=engine, target_schema='tmp', target_table='crashes_intersection_info', from_schema=next_tables[0], from_table=next_tables[1], partition_by_field='objectid', within_distance= 10)
print("intersection info query complete")
row_count = create_final_table(engine=engine, target_schema = 'analysis_data', target_table='dc_crashes_w_details', from_schema=next_tables[0], from_table=next_tables[1])
print("final query complete with row count ",row_count)