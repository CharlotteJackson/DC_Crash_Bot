import sqlalchemy
from connect_to_rds import get_connection_strings, create_postgres_engine
from add_location_info import add_location_info,add_school_info,add_walkscore_info,add_intersection_info,add_roadway_info,is_national_park,create_final_table
import argparse

def generate_crashes_all_sources (engine, **kwargs):

    # limit crashes table and create flags
    filter_crashes_query="""
    DROP TABLE IF EXISTS tmp.pulsepoint_dupe_check;
    CREATE TABLE tmp.pulsepoint_dupe_check 
    AS (
        SELECT DISTINCT a.* , 
            case 
                when b.incident_id is not null then 1 else 0 end as Potential_Duplicate_Incident_Flag
        FROM tmp.pulsepoint_filtered a
        LEFT JOIN tmp.pulsepoint_filtered b on a.incident_id <> b.incident_id 
        and (b.call_received_datetime at time zone 'America/New_York')::date = (a.call_received_datetime at time zone 'America/New_York')::date
        and abs(extract(epoch from b.call_received_datetime - a.call_received_datetime)/60) <=20.00
        and ST_DWithin(a.geography, b.geography, 100)
        and a.Agency_ID = b.Agency_ID
        and a.Incident_Type = b.Incident_Type
        --and (a.num_units_responding = 0 or a.unit_ids && b.unit_ids)

    ) ;
    """

    match_pulsepoint_query = """
    drop table if exists tmp.citizen_join_step1;
        create table tmp.citizen_join_step1 as (
        select * from (
            select distinct a.*
                ,b.geography as citizen_geography
                ,ST_Distance(a.geography, b.geography) as Distance_To_Citizen_Incident
                ,row_number() over (partition by a.incident_id order by ST_Distance(a.geography, b.geography)) as Distance_Rank
                ,abs(extract(epoch from b.cs - a.call_received_datetime)/60.00 ) as Minutes_Apart
                ,b.incident_key
                ,b.incident_desc_raw
                ,b.motorcycle_flag as citizen_motorcycle_flag
                ,b.someone_outside_car_struck as citizen_someone_outside_car_struck
            from tmp.pulsepoint_final_scanner_audio_matches a
            inner join source_data.citizen b on ST_DWITHIN(a.geography, b.geography, 500) 
                and abs(extract(epoch from b.cs - a.call_received_datetime)/60 )<60
            ) as tmp where Distance_Rank = 1
        ); 

    drop table if exists tmp.citizen_join_step2;
        create table tmp.citizen_join_step2 as (
        select * from (
            select distinct *
                ,row_number() over (partition by incident_key order by ST_Distance(geography, citizen_geography)) as Citizen_Distance_Rank
            from tmp.citizen_join_step1
            ) as tmp where Citizen_Distance_Rank = 1
        ); 

    drop table if exists tmp.citizen_pulsepoint_join_final;
    create table tmp.citizen_pulsepoint_join_final as 
    select 
        incident_id
        ,incident_type
        ,call_received_datetime
        ,fulldisplayaddress
        ,geography
        ,Potential_Duplicate_Incident_Flag
        ,someone_outside_car_struck
        ,motorcycle_flag
        ,car_crash_call
        ,call_ids_array
        ,transcripts_array
        ,incident_key
        ,incident_desc_raw
        ,citizen_someone_outside_car_struck
    from tmp.citizen_join_step2
    ;

    insert into tmp.citizen_pulsepoint_join_final
    select 
        incident_id
        ,incident_type
        ,call_received_datetime
        ,fulldisplayaddress
        ,geography
        ,Potential_Duplicate_Incident_Flag
        ,someone_outside_car_struck
        ,motorcycle_flag
        ,car_crash_call
        ,call_ids_array
        ,transcripts_array
        ,NULL as incident_key
        ,NULL as incident_desc_raw
        ,NULL as citizen_someone_outside_car_struck
    from tmp.pulsepoint_final_scanner_audio_matches 
    where incident_id not in (select incident_id from tmp.citizen_pulsepoint_join_final)
    ;
    """

    match_twitter_query = """

        drop table if exists tmp_twitter;
        create temporary table tmp_twitter on commit preserve rows as (
        select * from source_data.twitter_stream
        where ((tweet_text ilike '%%pedestrian%%' and tweet_text not ilike '%%pedestrian bridge%%')
            or tweet_text ilike '%% cyclist%%'
            or tweet_text ilike '%%bicycl%%'
            or tweet_text ilike '%%ped struck%%'
        or ((tweet_text ilike '%%struck by%%' or tweet_text ilike '%%hit by%%')
            and tweet_text not ilike '%%gunfire%%' 
            and tweet_text not ilike '%%bullets%%'
        and tweet_text not ilike '%%train%%'
        ))
        and (created_at at time zone 'America/New_York')::date >='2021-05-15' and point_geography is not null
            ) with data;

       drop table if exists tmp.twitter_join_step1;
        create table tmp.twitter_join_step1 as (
        select * from (
            select distinct a.*
                ,b.point_geography as twitter_geography
                ,ST_Distance(a.geography, b.point_geography) as Distance_To_twitter_Incident
                ,row_number() over (partition by a.incident_id order by ST_Distance(a.geography, b.point_geography)) as Distance_Rank
                ,abs(extract(epoch from b.created_at - a.call_received_datetime)/60.00 ) as Minutes_Apart
                ,b.tweet_id
                ,b.tweet_text
                ,1 as twitter_someone_outside_car_struck
            from tmp.citizen_pulsepoint_join_final a
            inner join tmp_twitter b on (ST_DWITHIN(a.geography, b.point_geography, 1000) 
								 or (ST_Area(b.polygon_geography::geography)<= 3000000 and ST_Intersects(b.polygon_geography, a.geography)))
		    and (extract(epoch from b.created_at - a.call_received_datetime)/60.00 )between 0 and 30
            ) as tmp where Distance_Rank = 1
        ); 

    drop table if exists tmp.twitter_join_step2;
        create table tmp.twitter_join_step2 as (
        select * from (
            select distinct *
                ,row_number() over (partition by tweet_id order by ST_Distance(geography, twitter_geography)) as twitter_Distance_Rank
            from tmp.twitter_join_step1
            ) as tmp where twitter_Distance_Rank = 1
        ); 

    drop table if exists tmp.twitter_pulsepoint_join_final;
    create table tmp.twitter_pulsepoint_join_final as 
    select 
        incident_id
        ,incident_type
        ,call_received_datetime
        ,fulldisplayaddress
        ,geography
        ,Potential_Duplicate_Incident_Flag
        ,someone_outside_car_struck
        ,motorcycle_flag
        ,car_crash_call
        ,call_ids_array
        ,transcripts_array
        ,incident_key
        ,incident_desc_raw
        ,citizen_someone_outside_car_struck
        ,tweet_id
        ,tweet_text
        ,twitter_someone_outside_car_struck
        ,case when twitter_someone_outside_car_struck = 1 or someone_outside_car_struck = 1 or citizen_someone_outside_car_struck = 1 then 1 else 0 end as Ped_Crash_Any_Source
    from tmp.twitter_join_step2
    ;

    insert into tmp.twitter_pulsepoint_join_final
    select 
        incident_id
        ,incident_type
        ,call_received_datetime
        ,fulldisplayaddress
        ,geography
        ,Potential_Duplicate_Incident_Flag
        ,someone_outside_car_struck
        ,motorcycle_flag
        ,car_crash_call
        ,call_ids_array
        ,transcripts_array
        ,incident_key
        ,incident_desc_raw
        ,citizen_someone_outside_car_struck
        ,NULL as tweet_id
        ,NULL as tweet_text
        ,NULL as twitter_someone_outside_car_struck
        ,case when someone_outside_car_struck = 1 or citizen_someone_outside_car_struck = 1 then 1 else 0 end as Ped_Crash_Any_Source
    from tmp.citizen_pulsepoint_join_final
    where incident_id not in (select incident_id from tmp.twitter_pulsepoint_join_final)
    ;

    CREATE INDEX IF NOT EXISTS pulsepoint_twitter_geom_idx ON tmp.twitter_pulsepoint_join_final USING GIST (geography);
    """

    # First execute the table-specific queries
    # engine.execute(dupe_check_query)
    print("dupe check query complete")

    engine.execute(match_pulsepoint_query)
    print("twitter matches processed")

    # Then execute the same location-info queries (roadway, schools, neighborhoods) that apply to all analysis tables and create the final table
    next_tables = add_location_info(engine=engine, target_schema='tmp', target_table='pulsepoint_nbh_ward', from_schema='tmp', from_table='twitter_pulsepoint_join_final', partition_by_field='Incident_ID')
    print("neighborhood-ward query complete")
    next_tables = add_roadway_info(engine=engine, target_schema='tmp', target_table='pulsepoint_roadway_info', from_schema=next_tables[0], from_table=next_tables[1], partition_by_field='Incident_ID', within_distance= 100)
    print("roadway info query complete")
    next_tables = add_intersection_info(engine=engine, target_schema='tmp', target_table='pulsepoint_intersection_info', from_schema=next_tables[0], from_table=next_tables[1], partition_by_field='Incident_ID', within_distance= 60)
    print("intersection info query complete")
    next_tables = is_national_park(engine=engine, target_schema='tmp', target_table='pulsepoint_national_park', from_schema=next_tables[0], from_table=next_tables[1])
    print("national parks info query complete")
    row_count = create_final_table(engine=engine, target_schema = 'analysis_data', target_table='pulsepoint', from_schema=next_tables[0], from_table=next_tables[1])
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
    generate_pulsepoint_analysis_table(engine=engine)