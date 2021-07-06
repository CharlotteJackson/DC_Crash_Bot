import sqlalchemy
from connect_to_rds import get_connection_strings, create_postgres_engine
from add_location_info import add_location_info,add_school_info,add_walkscore_info,add_intersection_info,add_roadway_info,is_national_park,create_final_table
import argparse

def generate_crashes_all_sources (engine, **kwargs):

    # limit crashes table and create flags
    filter_crashes_query="""
    DROP TABLE IF EXISTS tmp.mpd_reported_crashes;
    CREATE TABLE tmp.mpd_reported_crashes 
    AS (
        SELECT DISTINCT
        crimeid
        ,fromdate::date
        ,reportdate
        ,address
        ,total_vehicles
        ,total_bicyclists
        ,total_pedestrians
        ,total_injuries
        ,persontype_array
        ,invehicletype_array
        ,geography
        ,case when invehicletype_array::text ilike '%%motor%%cycle%%' then 1 else 0 end as mpd_motorcycle_flag 
        ,ward_name
        ,comp_plan_area
        ,anc_id
        ,smd_id
        ,nbh_cluster_names
        ,blockkey
        ,case when total_bicyclists > 0 or total_pedestrians > 0 or invehicletype_array::text ilike '%%moped%%'or invehicletype_array::text ilike '%%scooter%%'
            then 1 else 0 end as mpd_someone_outside_car_struck
        from analysis_data.dc_crashes_w_details
        where fromdate::date >= '2021-05-15'
    ) ;

    CREATE INDEX IF NOT EXISTS crashes_filtered_geom_idx ON tmp.mpd_reported_crashes USING GIST (geography);
    """

    match_pulsepoint_query = """
    
        drop table if exists tmp.mpd_crashes_pulsepoint_join_step1;
        create table tmp.mpd_crashes_pulsepoint_join_step1 as (
            select * from (
            select distinct a.*
            ,b.geography as dcfems_geography
            ,b.Ped_Crash_Any_Source as DCFEMS_Ped_Crash
            ,b.incident_id
            ,b.Potential_Duplicate_Incident_Flag
            ,call_received_datetime
            ,fulldisplayaddress
            ,call_ids_array
            ,transcripts_array
            ,incident_key
            ,incident_desc_raw
            ,tweet_id
            ,tweet_text
            ,row_number() over (partition by incident_id order by ST_Distance(a.geography, b.geography)) as Incident_to_Police_Report_Distance_Rank
            ,(reportdate at time zone 'America/New_York')  - (CALL_RECEIVED_DATETIME at time zone 'America/New_York') as Time_To_Report
            ,ST_Distance(a.geography, b.geography) as Call_Distance
        FROM tmp.mpd_reported_crashes a
                INNER JOIN analysis_data.pulsepoint b on ST_DWITHIN(a.geography, b.geography, 200) 
                    AND fromdate::date =cast((call_received_datetime at time zone 'America/New_York') as date)
                    AND (CALL_RECEIVED_DATETIME at time zone 'America/New_York')  < (reportdate at time zone 'America/New_York') 
                ) as tmp where (Incident_to_Police_Report_Distance_Rank = 1)
        ) ;

        drop table if exists tmp.mpd_crashes_pulsepoint_join_step2;
        create table tmp.mpd_crashes_pulsepoint_join_step2 as (
            select * from (
                select *
                ,row_number() over (partition by crimeid order by ST_Distance(geography, dcfems_geography)) as Police_Report_To_Incident_Distance_rank
                from tmp.mpd_crashes_pulsepoint_join_step1
                ) as tmp where Police_Report_To_Incident_Distance_rank = 1
            ) ;
    """

    exclude_calls_query = """
    drop table if exists tmp.exclude_call_ids;
    create table tmp.exclude_call_ids as (
        select ARRAY_AGG(call_ids) as exclude_call_ids FROM 
        (select unnest(call_ids_array) as call_ids from tmp.mpd_crashes_pulsepoint_join_step2 where mpd_someone_outside_car_struck = 1
        INTERSECT
        select unnest(call_ids_array) as call_ids from tmp.mpd_crashes_pulsepoint_join_step2 where mpd_someone_outside_car_struck = 0 and DCFEMS_Ped_Crash = 1
    ) as tmp
        ) ;
    """

    combine_tables_query = """
        drop table if exists tmp.crashes_all_sources;
        create table tmp.crashes_all_sources as
            (
            SELECT
                'MPD Only' as category
                ,'' as sub_category
                ,mpd_someone_outside_car_struck as MPD_Reports_Ped_Involved
                ,mpd_motorcycle_flag as MPD_Reports_Motorcycle_Involved
                ,0 as Other_Sources_Report_Ped_Involved
                ,crimeid as crash_id
                ,NULL as incident_id
                ,fromdate as accident_date
                ,address as MPD_Reported_Address
                ,NULL as DCFEMS_Call_Address
                ,total_bicyclists as MPD_Reported_Bicyclists
                ,total_pedestrians as MPD_Reported_Pedestrians
                ,persontype_array
                ,invehicletype_array
                ,ARRAY[NULL] as scanner_audio
                ,ARRAY[NULL] as scanner_call_ids
                ,NULL as citizen_description
                ,NULL as twitter_description
                ,ST_Y(geography) as MPD_latitude
                ,ST_X(geography) as MPD_longitude
                ,geography::geography as MPD_Location
                ,NULL::numeric as DCFEMS_Call_latitude
                ,NULL::numeric as DCFEMS_Call_longitude
                ,NULL::geography as DCFEMS_Call_Location
                ,geography
                ,ward_name
                ,comp_plan_area
                ,anc_id
                ,smd_id
                ,nbh_cluster_names
                ,blockkey
                ,ST_Y(geography) as master_latitude
                ,ST_X(geography) as master_longitude
                ,crimeid as unique_row_id
            FROM tmp.mpd_reported_crashes 
            WHERE crimeid not in (select crimeid from tmp.mpd_crashes_pulsepoint_join_step2)
            );

           insert into tmp.crashes_all_sources
            	SELECT
                'DCFEMS Only' as category
                ,'' as sub_category
                ,0 as MPD_Reports_Ped_Involved
                ,0 as MPD_Reports_Motorcycle_Involved
                ,Ped_Crash_Any_Source as Other_Sources_Report_Ped_Involved
                ,NULL as crash_id
                ,incident_id
                ,call_received_datetime::date as accident_date
                ,NULL as MPD_Reported_Address
                ,fulldisplayaddress as DCFEMS_Call_Address
                ,NULL as MPD_Reported_Bicyclists
                ,NULL as MPD_Reported_Pedestrians
                ,ARRAY[NULL] as persontype_array
                ,ARRAY[NULL] as invehicletype_array
                ,transcripts_array as scanner_audio
                ,call_ids_array as scanner_call_ids
                ,incident_desc_raw as citizen_description
                ,tweet_text as twitter_description
                ,NULL as MPD_latitude
                ,NULL as MPD_longitude
                ,NULL as MPD_Location
                ,ST_Y(geography::geometry) as DCFEMS_Call_latitude
                ,ST_X(geography::geometry) as DCFEMS_Call_longitude
                ,geography as DCFEMS_Call_Location
                ,geography::geometry 
                ,ward_name
                ,comp_plan_area
                ,anc_id
                ,smd_id
                ,nbh_cluster_names
                ,roadway_blockkey
                ,ST_Y(geography::geometry) as master_latitude
                ,ST_X(geography::geometry) as master_longitude
                ,incident_id as unique_row_id
            FROM analysis_data.pulsepoint 
            WHERE incident_id not in (select incident_id from tmp.mpd_crashes_pulsepoint_join_step2)
            ;

        insert into tmp.crashes_all_sources
        SELECT
            'DCFEMS and MPD' as category
            ,'No crash type conflict' as sub_category
            ,mpd_someone_outside_car_struck as MPD_Reports_Ped_Involved
            ,mpd_motorcycle_flag as MPD_Reports_Motorcycle_Involved
            ,case when b.exclude_call_ids is not null then 0 else DCFEMS_Ped_Crash end as Other_Sources_Report_Ped_Involved
            ,crimeid
            ,incident_id
            ,(call_received_datetime at time zone 'America/New_York')::date as accident_date
            ,address as MPD_Reported_Address
            ,fulldisplayaddress as DCFEMS_Call_Address
            ,total_bicyclists as MPD_Reported_Bicyclists
            ,total_pedestrians as MPD_Reported_Pedestrians
            ,persontype_array
            ,invehicletype_array
            ,transcripts_array as scanner_audio
            ,call_ids_array as scanner_call_ids
            ,incident_desc_raw as citizen_description
            ,tweet_text as twitter_description
            ,ST_Y(geography) as MPD_latitude
            ,ST_X(geography) as MPD_longitude
            ,geography::geography as MPD_Location
            ,ST_Y(dcfems_geography::geometry) as DCFEMS_Call_latitude
            ,ST_X(dcfems_geography::geometry) as DCFEMS_Call_longitude
            ,dcfems_geography as DCFEMS_Call_Location
            ,geography
            ,ward_name
            ,comp_plan_area
            ,anc_id
            ,smd_id
            ,nbh_cluster_names
            ,blockkey
            ,ST_Y(geography) as master_latitude
            ,ST_X(geography) as master_longitude
            ,crimeid as unique_row_id
        FROM tmp.mpd_crashes_pulsepoint_join_step2 a
        left join tmp.exclude_call_ids b on a.call_ids_array && b.exclude_call_ids
        WHERE  mpd_someone_outside_car_struck = 1 or (mpd_someone_outside_car_struck = 0 and DCFEMS_Ped_Crash = 0)
        or (mpd_someone_outside_car_struck = 0 and b.exclude_call_ids is not null)
        ;

    insert into tmp.crashes_all_sources
        SELECT
            'DCFEMS and MPD' as category
		    ,'Other sources report a pedestrian or cyclist crash; MPD reports motorcycle involved' as sub_category
            ,mpd_someone_outside_car_struck as MPD_Reports_Ped_Involved
            ,mpd_motorcycle_flag as MPD_Reports_Motorcycle_Involved
            ,DCFEMS_Ped_Crash as Other_Sources_Report_Ped_Involved
            ,crimeid
            ,incident_id
            ,call_received_datetime::date as accident_date
            ,address as MPD_Reported_Address
            ,fulldisplayaddress as DCFEMS_Call_Address
            ,total_bicyclists as MPD_Reported_Bicyclists
            ,total_pedestrians as MPD_Reported_Pedestrians
            ,persontype_array
            ,invehicletype_array
            ,transcripts_array as scanner_audio
            ,call_ids_array as scanner_call_ids
            ,incident_desc_raw as citizen_description
            ,tweet_text as twitter_description
            ,ST_Y(geography) as MPD_latitude
            ,ST_X(geography) as MPD_longitude
            ,geography::geography as MPD_Location
            ,ST_Y(dcfems_geography::geometry) as DCFEMS_Call_latitude
            ,ST_X(dcfems_geography::geometry) as DCFEMS_Call_longitude
            ,dcfems_geography as DCFEMS_Call_Location
            ,geography
            ,ward_name
            ,comp_plan_area
            ,anc_id
            ,smd_id
            ,nbh_cluster_names
            ,blockkey
            ,ST_Y(geography) as master_latitude
            ,ST_X(geography) as master_longitude
            ,crimeid as unique_row_id
        FROM tmp.mpd_crashes_pulsepoint_join_step2 
        WHERE  mpd_someone_outside_car_struck = 0 and DCFEMS_Ped_Crash = 1 and mpd_motorcycle_flag = 1
        ;

        insert into tmp.crashes_all_sources
        SELECT
            'DCFEMS and MPD' as category
		    ,'Other sources report a pedestrian or cyclist crash; MPD reports no non-vehicle parties involved' as sub_category
            ,mpd_someone_outside_car_struck as MPD_Reports_Ped_Involved
            ,mpd_motorcycle_flag as MPD_Reports_Motorcycle_Involved
            ,DCFEMS_Ped_Crash as Other_Sources_Report_Ped_Involved
            ,crimeid
            ,incident_id
            ,call_received_datetime::date as accident_date
            ,address as MPD_Reported_Address
            ,fulldisplayaddress as DCFEMS_Call_Address
            ,total_bicyclists as MPD_Reported_Bicyclists
            ,total_pedestrians as MPD_Reported_Pedestrians
            ,persontype_array
            ,invehicletype_array
            ,transcripts_array as scanner_audio
            ,call_ids_array as scanner_call_ids
            ,incident_desc_raw as citizen_description
            ,tweet_text as twitter_description
            ,ST_Y(geography) as MPD_latitude
            ,ST_X(geography) as MPD_longitude
            ,geography::geography as MPD_Location
            ,ST_Y(dcfems_geography::geometry) as DCFEMS_Call_latitude
            ,ST_X(dcfems_geography::geometry) as DCFEMS_Call_longitude
            ,dcfems_geography as DCFEMS_Call_Location
            ,geography
            ,ward_name
            ,comp_plan_area
            ,anc_id
            ,smd_id
            ,nbh_cluster_names
            ,blockkey
            ,ST_Y(geography) as master_latitude
            ,ST_X(geography) as master_longitude
            ,crimeid as unique_row_id
        FROM tmp.mpd_crashes_pulsepoint_join_step2  a
        left join tmp.exclude_call_ids b on a.call_ids_array && b.exclude_call_ids
        WHERE  mpd_someone_outside_car_struck = 0 and DCFEMS_Ped_Crash = 1 and mpd_motorcycle_flag = 0
        and b.exclude_call_ids is null
        ;
    """

    # First execute the table-specific queries
    engine.execute(filter_crashes_query)
    print("filter crashes query complete")

    engine.execute(match_pulsepoint_query)
    print("pulsepoint matches processed")

    engine.execute(exclude_calls_query)
    print("exclude calls query processed")

    engine.execute(combine_tables_query)
    print("tables combined")

    # create the final table
    row_count = create_final_table(engine=engine, target_schema = 'analysis_data', target_table='crashes_all_sources', from_schema='tmp', from_table='crashes_all_sources')
    print("final query complete with row count ",row_count)


CLI=argparse.ArgumentParser()
CLI.add_argument(
"--env",
type=str
)


# parse the command line
args = CLI.parse_args()
env=args.env

if __name__ == "__main__":
    if env == None:
        env = 'DEV'
    env = env.upper()
    # tables_to_extract = json_to_postGIS(folder_to_load='source-data/citizen/unparsed/', move_to_folder = 'source-data/citizen/loaded_to_postgis/', AWS_Credentials=get_connection_strings("AWS_DEV"))
    engine = create_postgres_engine(destination="AWS_PostGIS", env=env)
    generate_crashes_all_sources(engine=engine)