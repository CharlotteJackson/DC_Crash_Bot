import sqlalchemy
from connect_to_rds import get_connection_strings, create_postgres_engine
from add_location_info import add_location_info,add_school_info,add_walkscore_info,add_intersection_info,add_roadway_info,is_national_park,create_final_table
import argparse

def generate_pulsepoint_analysis_table (AWS_Credentials:dict, **kwargs):

    # if no environment is specified default to dev 
    env=kwargs.get('env', None)
    if env == None:
        env='DEV'
    env=env.upper()

    # set up RDS and S3 connections, engines, cursors
    region=AWS_Credentials['region']
    engine = create_postgres_engine(destination="AWS_PostGIS", env=env)

    # flag that some records might be duplicate calls for the same incident 
    dupe_check_query="""
    DROP TABLE IF EXISTS tmp.pulsepoint_dupe_check;
    CREATE TABLE tmp.pulsepoint_dupe_check 
    AS (
        SELECT DISTINCT a.* , 
            case 
                when b.incident_id is null then 1 
                when a.num_units_responding = 0 and b.num_units_responding >0 then 0 
                when b.unit_status_transport > a.unit_status_transport then 0
                when b.num_units_responding > a.num_units_responding then 0 
                when b.call_received_datetime < a.call_received_datetime then 0
                else 1 end as KEEP_RECORD_FLAG
        FROM source_data.pulsepoint a
        LEFT JOIN source_data.pulsepoint b on a.incident_id <> b.incident_id 
        and date_part('day', a.call_received_datetime - b.call_received_datetime) = 0
        and date_part('hour', a.call_received_datetime - b.call_received_datetime) = 0
        and date_part('month', a.call_received_datetime - b.call_received_datetime) = 0
        and abs(date_part('minute', a.call_received_datetime - b.call_received_datetime)) <=20
        and ST_DWithin(a.geography, b.geography, 100)
        and a.Agency_ID = b.Agency_ID
        and (a.num_units_responding = 0 or a.unit_ids && b.unit_ids)

    ) ;

    CREATE INDEX IF NOT EXISTS pulsepoint_dupe_check_geom_idx ON tmp.pulsepoint_dupe_check USING GIST (geography);
    """

    # then join to the crashes table 
    crashes_join_query="""


    DROP TABLE IF EXISTS tmp.pulsepoint_crash_join;
    CREATE TABLE tmp.pulsepoint_crash_join 
    AS (SELECT * 
    FROM (
        SELECT DISTINCT a.* 
        ,concat(a.agency_id, a.incident_id) as Agency_Incident_ID
            ,b.objectid as Crash_Objectid 
            ,b.geography as Crash_Geo
            ,b.total_injuries as Crash_Total_Injuries
            ,b.total_major_injuries as Crash_Total_Major_Injuries 
            ,b.total_minor_injuries as Crash_Total_Minor_Injuries 
            ,(b.bicycle_fatalities + b.pedestrian_fatalities + b.vehicle_fatalities) as Crash_Total_Fatalities
            ,b.bicycle_injuries as Crash_Bike_Injuries
            ,b.vehicle_injuries as Crash_Car_Injuries
            ,b.pedestrian_injuries as Crash_Ped_Injuries
            ,case when b.total_injuries is null or b.total_injuries < a.unit_status_transport then 1 else 0 end as injuries_mismatch
            ,ST_Distance(a.geography, b.geography) as Distance_To_Crash
            ,(b.reportdate at time zone 'America/New_York')  - (a.CALL_RECEIVED_DATETIME at time zone 'America/New_York')  as Time_Between_Crash_And_Report
            ,b.intersectionid as Crash_Intersection_ID
            ,b.block_objectid as Crash_Block_Objectid
            ,Row_Number() over (partition by a.incident_id, a.agency_id order by ST_Distance(a.geography, b.geography)) as Crash_Distance_Rank
            ,Row_Number() over (partition by a.incident_id, a.agency_id order by (b.reportdate at time zone 'America/New_York')  - (a.CALL_RECEIVED_DATETIME at time zone 'America/New_York')) as Crash_Time_Rank
        FROM tmp.pulsepoint_dupe_check a
        LEFT JOIN analysis_data.dc_crashes_w_details b on ST_DWITHIN(a.geography, b.geography, 200) 
            AND cast(b.fromdate as date) =cast((call_received_datetime at time zone 'America/New_York') as date)
            AND (a.CALL_RECEIVED_DATETIME at time zone 'America/New_York')  < (b.reportdate at time zone 'America/New_York') 
        WHERE a.KEEP_RECORD_FLAG = 1
    ) tmp WHERE Crash_Distance_Rank = 1 and (incident_type in ('TC', 'TCE', 'TCS') or (agency_id = '16000' and incident_type in ('TC', 'TCS', 'TCE', 'RES')))
    ) ;

    CREATE INDEX IF NOT EXISTS pulsepoint_crash_join_geom_idx ON tmp.pulsepoint_crash_join USING GIST (geography);

    alter table tmp.pulsepoint_crash_join drop column KEEP_RECORD_FLAG;
    alter table tmp.pulsepoint_crash_join drop column Crash_Distance_Rank;
    """

    # First execute the table-specific queries
    engine.execute(dupe_check_query)
    print("dupe check query complete")

    engine.execute(crashes_join_query)
    print("join to crashes query complete")

    # Then execute the same location-info queries (roadway, schools, neighborhoods) that apply to all analysis tables and create the final table
    next_tables = add_location_info(engine=engine, target_schema='tmp', target_table='pulsepoint_nbh_ward', from_schema='tmp', from_table='pulsepoint_crash_join', partition_by_field='Agency_Incident_ID')
    print("neighborhood-ward query complete")
    next_tables = add_school_info(engine=engine, target_schema='tmp', target_table='pulsepoint_schools', from_schema=next_tables[0], from_table=next_tables[1])
    print("schools query complete")
    next_tables = add_walkscore_info(engine=engine, target_schema='tmp', target_table='pulsepoint_walkscore', from_schema=next_tables[0], from_table=next_tables[1])
    print("walkscore query complete")
    next_tables = add_roadway_info(engine=engine, target_schema='tmp', target_table='pulsepoint_roadway_info', from_schema=next_tables[0], from_table=next_tables[1], partition_by_field='Agency_Incident_ID', within_distance= 100)
    print("roadway info query complete")
    next_tables = add_intersection_info(engine=engine, target_schema='tmp', target_table='pulsepoint_intersection_info', from_schema=next_tables[0], from_table=next_tables[1], partition_by_field='Agency_Incident_ID', within_distance= 60)
    print("intersection info query complete")
    next_tables = is_national_park(engine=engine, target_schema='tmp', target_table='pulsepoint_national_park', from_schema=next_tables[0], from_table=next_tables[1])
    print("national parks info query complete")
    row_count = create_final_table(engine=engine, target_schema = 'analysis_data', target_table='pulsepoint', from_schema=next_tables[0], from_table=next_tables[1])
    print("final query complete with row count ",row_count)


# command line arguments
CLI=argparse.ArgumentParser()
CLI.add_argument(
"--env",
type=str
)

# parse the command line
args = CLI.parse_args()
env=args.env

if __name__ == "__main__":
    generate_pulsepoint_analysis_table(AWS_Credentials=get_connection_strings("AWS_DEV"), env=env)