import sqlalchemy
from connect_to_rds import get_connection_strings, create_postgres_engine
from add_location_info import add_location_info,add_school_info,add_walkscore_info,add_intersection_info,add_roadway_info,is_national_park,create_final_table
import argparse

def generate_pulsepoint_analysis_table (engine, **kwargs):

    # flag that some records might be duplicate calls for the same incident 
    dupe_check_query="""
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

    # then join to the crashes table 
    match_scanner_audio_query="""
    drop table if exists tmp.pulsepoint_to_scanner_audio_join_step1;
        create table tmp.pulsepoint_to_scanner_audio_join_step1 as (
            select distinct a.*
            ,b.call_id
            ,b.call_timestamp
            ,b.main_transcript_cleaned
            ,b.call_length
            ,b.unit_numbers_array
            ,b.call_addresses
            ,b.car_crash_call
            ,b.someone_outside_car_struck 
            ,b.motorcycle_flag
            ,1 as join_round
            ,'unit number and address' as join_type
        from tmp.pulsepoint_dupe_check a
            inner join tmp.pulsepoint_addresses_processed adr on a.incident_id = adr.incident_id
            inner join tmp.pulsepoint_unit_ids units on a.incident_id = units.incident_id
            inner join source_data.scanner_audio_classified b on 
                extract(epoch from b.call_timestamp - a.call_received_datetime)/60 between -1.000 and 15.000
                and units.unit_id = ANY(b.unit_numbers_array) and units.call_date = (b.call_timestamp at time zone 'America/New_York')::date
                and adr.fulldisplayaddress = ANY(b.call_addresses)
                and adr.call_date = (b.call_timestamp at time zone 'America/New_York')::date
            );

            insert into tmp.pulsepoint_to_scanner_audio_join_step1
            select distinct a.*
                ,b.call_id
                ,b.call_timestamp
                ,b.main_transcript_cleaned
                ,b.call_length
                ,b.unit_numbers_array
                ,b.call_addresses
                ,b.car_crash_call
                ,b.someone_outside_car_struck
                ,b.motorcycle_flag
                ,2 as join_round
                ,'unit number' as join_type
            from tmp.pulsepoint_dupe_check a
                inner join tmp.pulsepoint_unit_ids  units on a.incident_id = units.incident_id
                inner join source_data.scanner_audio_classified  b on 
                    extract(epoch from b.call_timestamp - a.call_received_datetime)/60 between -1.000 and 15.000
                    and units.unit_id = ANY(b.unit_numbers_array) 
                    and units.call_date = (b.call_timestamp at time zone 'America/New_York')::date
            where a.incident_id not in (select distinct incident_id from tmp.pulsepoint_to_scanner_audio_join_step1)
            ;

            insert into tmp.pulsepoint_to_scanner_audio_join_step1
            select distinct a.*
                ,b.call_id
                ,b.call_timestamp
                ,b.main_transcript_cleaned
                ,b.call_length
                ,b.unit_numbers_array
                ,b.call_addresses
                ,b.car_crash_call
                ,b.someone_outside_car_struck
                ,b.motorcycle_flag
                ,3 as join_round
                ,'address' as join_type
            from tmp.pulsepoint_dupe_check a
                inner join tmp.pulsepoint_addresses_processed adr on a.incident_id = adr.incident_id
                inner join source_data.scanner_audio_classified b on 
                    extract(epoch from b.call_timestamp - a.call_received_datetime)/60 between -1.000 and 15.000
                    and adr.fulldisplayaddress = ANY(b.call_addresses) 
                    and adr.call_date = (b.call_timestamp at time zone 'America/New_York')::date
            where a.incident_id not in (select distinct incident_id from tmp.pulsepoint_to_scanner_audio_join_step1) 
                and (b.unit_numbers_array[1] is null or b.call_length>=25)
                ;

            insert into tmp.pulsepoint_to_scanner_audio_join_step1
                select distinct a.*
                ,b.call_id
                ,b.call_timestamp
                ,b.main_transcript_cleaned
                ,b.call_length
                ,b.unit_numbers_array
                ,b.call_addresses
                ,b.car_crash_call
                ,b.someone_outside_car_struck
                ,b.motorcycle_flag
                ,4 as join_round
                ,'quadrant and call type' as join_type
            from tmp.pulsepoint_dupe_check a
                inner join tmp.pulsepoint_quadrants quadrants on a.incident_id = quadrants.incident_id
                inner join source_data.scanner_audio_classified  b on 
                    extract(epoch from b.call_timestamp - a.call_received_datetime)/60 between -0.5 and 1.5
                    and quadrants.quadrant = ANY(b.call_quadrants) and quadrants.call_date = (b.call_timestamp at time zone 'America/New_York')::date
                    and (case when a.incident_type in ('TC', 'TCE', 'TCS', 'VF') then 1 else 0 end = b.car_crash_call
                    or (a.incident_type in ('ME', 'EM') and b.someone_outside_car_struck = 1))
            where a.incident_id not in (select distinct incident_id from tmp.pulsepoint_to_scanner_audio_join_step1 where join_round in (1,2)) 
            and ((b.call_addresses[1] is null and b.unit_numbers_array[1] is null) or b.call_length>=25)   
            ;

            insert into tmp.pulsepoint_to_scanner_audio_join_step1
                select distinct a.*
                ,b.call_id
                ,b.call_timestamp
                ,b.main_transcript_cleaned
                ,b.call_length
                ,b.unit_numbers_array
                ,b.call_addresses
                ,b.car_crash_call
                ,b.someone_outside_car_struck
                ,b.motorcycle_flag
                ,5 as join_round
                ,'call type only' as join_type
            from tmp.pulsepoint_dupe_check a
                inner join source_data.scanner_audio_classified b on 
                    extract(epoch from b.call_timestamp - a.call_received_datetime)/60 between -0.5 and 1.5
                    and (case when a.incident_type in ('TC', 'TCE', 'TCS', 'VF') then 1 else 0 end = b.car_crash_call
                    or (a.incident_type in ('ME', 'EM') and b.someone_outside_car_struck = 1))
            where a.incident_id not in (select distinct incident_id from tmp.pulsepoint_to_scanner_audio_join_step1 where join_round in (1,2)) 
            and ((b.call_addresses[1] is null and b.unit_numbers_array[1] is null and b.call_quadrants[1] is null) or b.call_length>=25)
            ;

            insert into tmp.pulsepoint_to_scanner_audio_join_step1
                select distinct a.*
                ,b.call_id
                ,b.call_timestamp
                ,b.main_transcript_cleaned
                ,b.call_length
                ,b.unit_numbers_array
                ,b.call_addresses
                ,b.car_crash_call
                ,b.someone_outside_car_struck
                ,b.motorcycle_flag
                ,6 as join_round
                ,'time only' as join_type
            from tmp.pulsepoint_dupe_check a
                inner join source_data.scanner_audio_classified b on 
                    extract(epoch from b.call_timestamp - a.call_received_datetime)/60 between 0 and 1.0
            where a.incident_id not in (select distinct incident_id from tmp.pulsepoint_to_scanner_audio_join_step1) 
            and b.call_id not in (select distinct call_id from tmp.pulsepoint_to_scanner_audio_join_step1)
            ;
    """

    process_scanner_audio_matches_query="""
    drop table if exists tmp.pulsepoint_to_scanner_audio_join_step2;
    create table tmp.pulsepoint_to_scanner_audio_join_step2 as (
        Select *
            ,max(case when incident_type in ('TC', 'TCE', 'TCS', 'VF') then 1 else 0 end) over (partition by call_id) as Call_Matches_Car_Crash_Incident
            ,max(car_crash_call) over (partition by incident_id) as Incident_Matches_Car_Crash_Call
            ,max(case when car_crash_call = 0 then 1 else 0 end) over (partition by incident_id) as Incident_Matches_Non_Car_Crash_Call
            ,min(case when car_crash_call = 0 then join_round else null end) over (partition by incident_id) as Non_Car_Crash_Call_Min_Join_Round
            ,min(case when car_crash_call = 1 then join_round else null end) over (partition by incident_id) as Car_Crash_Call_Min_Join_Round
            ,max(someone_outside_car_struck) over (partition by incident_id) as Incident_Matches_Ped_Struck_Call
            ,min(join_round) over (partition by call_id) as call_min_join_round
            ,min(join_round) over (partition by incident_id) as incident_min_join_round
            ,dense_rank() over (partition by call_id order by abs(extract(epoch from call_timestamp - call_received_datetime))) as time_delta_rank_audio
            ,dense_rank() over (partition by incident_id order by abs(extract(epoch from call_timestamp - call_received_datetime))) as time_delta_rank_pulsepoint
            ,dense_rank() over (partition by call_id, join_round order by abs(extract(epoch from call_timestamp - call_received_datetime))) as time_delta_rank_audio_round
            ,dense_rank() over (partition by incident_id, join_round order by abs(extract(epoch from call_timestamp - call_received_datetime))) as time_delta_rank_pulsepoint_round
        from tmp.pulsepoint_to_scanner_audio_join_step1
	);

    drop table if exists tmp.pulsepoint_scanner_audio_matches;
    create table tmp.pulsepoint_scanner_audio_matches as (
        select * from tmp.pulsepoint_to_scanner_audio_join_step2
        where join_round in (1,2)
        and someone_outside_car_struck = 1
        and (incident_type in ('TC', 'TCE', 'TCS', 'VF') 
            or (Incident_type in ('ME', 'EM') 
                and Call_Matches_Car_Crash_Incident = 0
                and Incident_Matches_Non_Car_Crash_Call = 0)
            )
        and (time_delta_rank_pulsepoint_round = 1 or time_delta_rank_audio_Round=1)
        );

    insert into tmp.pulsepoint_scanner_audio_matches
        select * 
        from tmp.pulsepoint_to_scanner_audio_join_step2 where someone_outside_car_struck = 1
        and incident_id not in (select incident_id from tmp.pulsepoint_scanner_audio_matches)
        and (incident_type in ('TC', 'TCE', 'TCS', 'VF') or (Incident_type in ('ME', 'EM') and Call_Matches_Car_Crash_Incident = 0
                                                            and Incident_Matches_Non_Car_Crash_Call = 0))	
        and (time_delta_rank_pulsepoint_round = 1 or (join_round<6 and time_delta_rank_audio_Round=1))
        and join_round = call_min_join_round 
        and call_min_join_round<=incident_min_join_round
        ;

    insert into tmp.pulsepoint_scanner_audio_matches
        select * 
        from tmp.pulsepoint_to_scanner_audio_join_step2 where someone_outside_car_struck = 1
        and incident_id not in (select incident_id from tmp.pulsepoint_scanner_audio_matches)
        and call_id not in (select call_id from tmp.pulsepoint_scanner_audio_matches)
        and (incident_type in ('TC', 'TCE', 'TCS', 'VF') or (Incident_type in ('ME', 'EM') and Call_Matches_Car_Crash_Incident = 0
                                                            and Incident_Matches_Non_Car_Crash_Call = 0))	
        and (time_delta_rank_pulsepoint_round = 1 or (join_round<6 and time_delta_rank_audio_Round=1))
        and join_round = incident_min_join_round 
        and call_min_join_round<=incident_min_join_round
        ;

    insert into tmp.pulsepoint_scanner_audio_matches
        select * from tmp.pulsepoint_to_scanner_audio_join_step2
        where join_round in (1,2)
        and incident_type in ('TC', 'TCE', 'TCS', 'VF') 
        and (time_delta_rank_pulsepoint_round = 1 or time_delta_rank_audio_Round=1)
        and incident_id not in (select incident_id from tmp.pulsepoint_scanner_audio_matches)
        and call_id not in (select call_id from tmp.pulsepoint_scanner_audio_matches)
        ;

    insert into tmp.pulsepoint_scanner_audio_matches
        select *
        from tmp.pulsepoint_to_scanner_audio_join_step2  
        where 
        incident_id not in (select incident_id from tmp.pulsepoint_scanner_audio_matches)
        and call_id not in (select call_id from tmp.pulsepoint_scanner_audio_matches)
        and incident_type in ('TC', 'TCE', 'TCS', 'VF') 
        and (time_delta_rank_pulsepoint_round = 1 or (join_round<6 and time_delta_rank_audio_Round=1))
        and join_round = call_min_join_round 
        and call_min_join_round<=incident_min_join_round
        order by incident_id
        ;

    insert into tmp.pulsepoint_scanner_audio_matches
        select *
        from tmp.pulsepoint_to_scanner_audio_join_step2  
        where 
        incident_id not in (select incident_id from tmp.pulsepoint_scanner_audio_matches)
        and call_id not in (select call_id from tmp.pulsepoint_scanner_audio_matches)
        and incident_type in ('TC', 'TCE', 'TCS', 'VF') 
        and (time_delta_rank_pulsepoint_round = 1 or (join_round<6 and time_delta_rank_audio_Round=1))
        and join_round = incident_min_join_round 
        and call_min_join_round<=incident_min_join_round
        order by incident_id
        ;

        drop table if exists tmp.pulsepoint_final_scanner_audio_matches;
        create table tmp.pulsepoint_final_scanner_audio_matches as (
        select
                incident_id
                ,incident_type
                ,call_received_datetime
                ,fulldisplayaddress
                ,geography
                ,Potential_Duplicate_Incident_Flag
                ,max(someone_outside_car_struck) as someone_outside_car_struck
                ,max(motorcycle_flag) as motorcycle_flag
                ,max(car_crash_call) as car_crash_call
                ,ARRAY_AGG(DISTINCT replace(call_id,'"','')) as call_ids_array
                ,ARRAY_AGG(DISTINCT main_transcript_cleaned) as transcripts_array
            from tmp.pulsepoint_scanner_audio_matches
            group by 
                incident_id
                ,incident_type
                ,call_received_datetime
                ,fulldisplayaddress
                ,geography
                ,Potential_Duplicate_Incident_Flag
        ); 

        insert into tmp.pulsepoint_final_scanner_audio_matches
        select incident_id
                ,incident_type
                ,call_received_datetime
                ,fulldisplayaddress
                ,geography
                ,Potential_Duplicate_Incident_Flag
                ,0 as someone_outside_car_struck
                ,0 as motorcycle_flag
                ,0 as car_crash_call
                ,ARRAY[NULL] as call_ids_array
                ,ARRAY[NULL] as transcripts_array
        from tmp.pulsepoint_dupe_check
        where incident_id not in (select incident_id from tmp.pulsepoint_final_scanner_audio_matches)
        and incident_type in ('TC', 'TCE', 'TCS', 'VF') 
        ;

    """

    match_citizen_query = """
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
    engine.execute(dupe_check_query)
    print("dupe check query complete")

    engine.execute(match_scanner_audio_query)
    print("match scanner audio query complete")

    engine.execute(process_scanner_audio_matches_query)
    print("scanner audio matches processed")

    engine.execute(match_citizen_query)
    print("citizen matches processed")

    engine.execute(match_twitter_query)
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