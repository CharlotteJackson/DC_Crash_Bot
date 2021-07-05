from connect_to_rds import get_connection_strings, create_postgres_engine
from json_to_postgis import json_to_postGIS
import argparse

def classify_scanner_audio (engine, **kwargs):

    # extract the json info
    get_pulsepoint_info_query ="""
    DROP TABLE IF EXISTS tmp.pulsepoint_filtered;
    CREATE TABLE tmp.pulsepoint_filtered
    AS ( 
        SELECT * FROM source_data.pulsepoint 
        WHERE (call_received_datetime at time zone 'America/New_York')::date >= '2021-05-15' and agency_id = 'EMS1205'
        );

        drop table if exists tmp.pulsepoint_unit_ids;
        create table tmp.pulsepoint_unit_ids as (
        select incident_id, (call_received_datetime at time zone 'America/New_York')::date as call_date, unnest(unit_ids) as unit_id
            from tmp.pulsepoint_filtered
        );

        drop table if exists tmp.pulsepoint_quadrants;
        create table tmp.pulsepoint_quadrants as (
            select incident_id
            , (call_received_datetime at time zone 'America/New_York')::date as call_date
            ,case 
                when replace(fulldisplayaddress,',',' ') like '%% SW %%' then 'south west'
                when replace(fulldisplayaddress,',',' ') like '%% NW %%' then 'north west'
                when replace(fulldisplayaddress,',',' ') like '%% NE %%' then 'north east'
                when replace(fulldisplayaddress,',',' ') like '%% SE %%' then 'south east'
            end as quadrant
            from tmp.pulsepoint_filtered
            UNION ALL
            select incident_id
            , (call_received_datetime at time zone 'America/New_York')::date
            ,case 
                when replace(fulldisplayaddress,',',' ') like '%% SW %%' then 'southwest'
                when replace(fulldisplayaddress,',',' ') like '%% NW %%' then 'northwest'
                when replace(fulldisplayaddress,',',' ') like '%% NE %%' then 'northeast'
                when replace(fulldisplayaddress,',',' ') like '%% SE %%' then 'southeast'
            end as quadrant
            from tmp.pulsepoint_filtered
        );

        drop table if exists tmp.pulsepoint_addresses;
        create table tmp.pulsepoint_addresses as (
        select incident_id, (call_received_datetime at time zone 'America/New_York')::date as call_date, fulldisplayaddress, fulldisplayaddress as original_address
            from tmp.pulsepoint_filtered
            where fulldisplayaddress not like '%%I695%%'
            and fulldisplayaddress not like '%%I295%%'
            and fulldisplayaddress not like '%%I395%%'
            and fulldisplayaddress not like '%%I66%%'
        );

        update tmp.pulsepoint_addresses set fulldisplayaddress = replace(fulldisplayaddress, ', WASHINGTON, DC',' ');
        update tmp.pulsepoint_addresses set fulldisplayaddress = replace(fulldisplayaddress, ',',' ');
        update tmp.pulsepoint_addresses set fulldisplayaddress = replace(fulldisplayaddress, '-',' ');
        update tmp.pulsepoint_addresses set fulldisplayaddress = regexp_replace(regexp_replace(fulldisplayaddress, ' STE [A-Z]{0,1}[0-9]{1,3}',''), '([0-9]{1,4} )','','g');
        update tmp.pulsepoint_addresses set fulldisplayaddress = replace(fulldisplayaddress, ' NW ',' ');
        update tmp.pulsepoint_addresses set fulldisplayaddress = replace(fulldisplayaddress, ' NE ',' ');
        update tmp.pulsepoint_addresses set fulldisplayaddress = replace(fulldisplayaddress, ' SW ',' ');
        update tmp.pulsepoint_addresses set fulldisplayaddress = replace(fulldisplayaddress, ' SE ',' ');
        update tmp.pulsepoint_addresses set fulldisplayaddress = replace(fulldisplayaddress, ' ST ',' STREET ');
        update tmp.pulsepoint_addresses set fulldisplayaddress = replace(fulldisplayaddress, ' AVE ',' AVENUE ');
        update tmp.pulsepoint_addresses set fulldisplayaddress = replace(fulldisplayaddress, ' RD ',' ROAD ');
        update tmp.pulsepoint_addresses set fulldisplayaddress = replace(fulldisplayaddress, ' DR ',' DRIVE ');
        update tmp.pulsepoint_addresses set fulldisplayaddress = replace(fulldisplayaddress, ' SQ ',' SQUARE ');
        update tmp.pulsepoint_addresses set fulldisplayaddress = replace(fulldisplayaddress, ' PL ',' PLACE ');
        update tmp.pulsepoint_addresses set fulldisplayaddress = replace(fulldisplayaddress, ' LN ',' LANE ');
        update tmp.pulsepoint_addresses set fulldisplayaddress = replace(fulldisplayaddress, ' PLZ ',' PLAZA ');
        update tmp.pulsepoint_addresses set fulldisplayaddress = replace(fulldisplayaddress, ' TER ',' TERRACE ');
        update tmp.pulsepoint_addresses set fulldisplayaddress = replace(fulldisplayaddress, ' ALY ',' ALLEY ');

        drop table if exists tmp.pulsepoint_addresses_processed;
        create  table tmp.pulsepoint_addresses_processed as (
            select incident_id, call_date, original_address,trim(fulldisplayaddress) as fulldisplayaddress
            from tmp.pulsepoint_addresses
            where fulldisplayaddress not ilike '%%\&%%'
        UNION 
            select incident_id, call_date, original_address,trim(left(fulldisplayaddress,strpos(fulldisplayaddress,'&')-1)) as fulldisplayaddress
            from tmp.pulsepoint_addresses
            where fulldisplayaddress ilike '%%\&%%' 
        UNION 
            select incident_id, call_date, original_address,trim(right(fulldisplayaddress,length(fulldisplayaddress)-strpos(fulldisplayaddress,'&'))) as fulldisplayaddress
            from tmp.pulsepoint_addresses
            where fulldisplayaddress ilike '%%\&%%' 
        ) ;

        insert into tmp.pulsepoint_addresses_processed
        SELECT incident_id, call_date, original_address, replace(fulldisplayaddress, 'CAPITOL','CAPITAL')
        from tmp.pulsepoint_addresses_processed
        where fulldisplayaddress like '%%CAPITOL%%';

        insert into tmp.pulsepoint_addresses_processed
        SELECT incident_id, call_date, original_address, replace(fulldisplayaddress, 'KING JR','KING JUNIOR')
        from tmp.pulsepoint_addresses_processed
        where fulldisplayaddress like '%%KING JR%%';

        insert into tmp.pulsepoint_addresses_processed
        SELECT incident_id, call_date, original_address, replace(fulldisplayaddress, 'LUTHER KING JR','LUTHER KING')
        from tmp.pulsepoint_addresses_processed
        where fulldisplayaddress like '%%KING JR%%';

        insert into tmp.pulsepoint_addresses_processed
        SELECT incident_id, call_date, original_address, 'SEA STREET'
        from tmp.pulsepoint_addresses_processed
        where fulldisplayaddress = 'C STREET';

        insert into tmp.pulsepoint_addresses_processed
        SELECT incident_id, call_date, original_address, 'SEE STREET'
        from tmp.pulsepoint_addresses_processed
        where fulldisplayaddress = 'C STREET';

        delete from tmp.pulsepoint_addresses_processed where length(fulldisplayaddress)<=4;
    """

    link_pulsepoint_info_to_scanner_audio_query="""
        drop table if exists tmp.scanner_unit_numbers;
        create table tmp.scanner_unit_numbers as (
            select 
                a.call_id
                ,a.call_timestamp
                ,a.call_length
                ,replace(replace(replace(a.main_transcript::text,'.',''),',',''),'"',' ') as main_transcript_cleaned
                ,main_transcript::varchar as main_transcript
                ,array_agg(distinct b.unit_number::text) as unit_numbers_array
            from source_data.openmhz a
            left join (select *
                    ,unnest(unit_number_audio) as audio_strings 
                    from source_data.dcfems_unit_numbers_lookup) b 
            on replace(replace(a.main_transcript::text,'.',''),',','') ilike concat('%%',audio_strings::text,'%%')
            where a.call_talkgroup = '101' 
                and (call_timestamp at time zone 'America/New_York')::date >= '2021-05-15'
            group by a.call_id, a.call_length, a.call_timestamp, replace(replace(replace(a.main_transcript::text,'.',''),',',''),'"',''),main_transcript::varchar
            );

        drop table if exists tmp.scanner_quadrants;
        create table tmp.scanner_quadrants as (
            select 
                a.call_id
                , a.call_timestamp
                , a.call_length
                , a.main_transcript
                , a.main_transcript_cleaned
                , a.unit_numbers_array
                ,array_agg(distinct b.quadrant::text) as call_quadrants
            from tmp.scanner_unit_numbers a
            left join tmp.pulsepoint_quadrants b on call_date = (call_timestamp at time zone 'America/New_York')::date and main_transcript_cleaned ilike concat('%%',b.quadrant,'%%') 
            group by  a.call_id, a.call_timestamp, a.call_length, a.main_transcript, a.main_transcript_cleaned, a.unit_numbers_array
            ) ;

        drop table if exists tmp.scanner_addresses;
        create table tmp.scanner_addresses as (
            select 
                a.call_id
                , a.call_timestamp
                , a.call_length
                , a.main_transcript
                , a.main_transcript_cleaned
                , a.unit_numbers_array
                ,a.call_quadrants
                ,array_agg(distinct b.fulldisplayaddress::text) as call_addresses
            from tmp.scanner_quadrants a
            left join tmp.pulsepoint_addresses_processed b on call_date = (call_timestamp at time zone 'America/New_York')::date 
            and main_transcript_cleaned ilike concat('%%',' ',b.fulldisplayaddress,'%%') 
            group by  a.call_id, a.call_timestamp, a.call_length, a.main_transcript, a.main_transcript_cleaned, a.unit_numbers_array, a.call_quadrants
            );
    """

    classify_scanner_calls_query = """
        drop table if exists tmp.flag_pedestrians;
        create table tmp.flag_pedestrians as (
            select distinct call_id
            ,main_transcript_cleaned
            ,regexp_matches(main_transcript_cleaned
                        ,'pedestrian|((?<!motor)cyclist)|the austrian'
                        ,'ig'
                        )
            from tmp.scanner_addresses
            UNION 
            select distinct call_id
                ,main_transcript_cleaned
                ,regexp_matches(main_transcript_cleaned
                                ,'((charles and my son)|industry|equestrian|participant|(the restaurants)|kardashian|(dash three)|(that story)|british|(that.{0,10}your)|condition|addition|national|elizabeth|basically|destin((ation){0,1}|y{0,1})|destry|actually|traditional|potentially|expedition|professional|definitely|credential|possession|refreshing|petition|protection|production|reductionist|investment|investing|((?<!motor)cycl))\D{0,10}?(instruct|truck|drug|stroke|interrupt)'
                                ,'ig'
                            )
                from tmp.scanner_addresses
            UNION 
            select distinct call_id
                ,main_transcript_cleaned
                ,regexp_matches(main_transcript_cleaned
                            ,'involv.{0,20}?((charles and my son)|industry|equestrian|participant|correction|kardashian|addition|condition|position|destin|destry|traditional|potentially|expedition|professional|definitely|credential|possession|refreshing|petition|protection|production|reductionist|investment|investing|((?<!motor)cycl))'
                            ,'ig'
                            )
            from tmp.scanner_addresses
            UNION
            select distinct call_id
                ,main_transcript_cleaned
                ,regexp_matches(main_transcript_cleaned
                            ,'((s.{0,1}?truck|drug)|stroke|hit).{1,3}?by.{1,3}?(vehicle|((metro){0,1}? {0,1}?bus)|car|motor)'
                            ,'ig'
                            )
            from tmp.scanner_addresses
            );

        drop table if exists tmp.flag_accidents;
        create table tmp.flag_accidents as (
            select distinct call_id
                ,main_transcript_cleaned
                ,regexp_matches(main_transcript_cleaned
                            ,'(((M|N) {0,1}?(V|B|C|T))|((e|E)nvy)).{0,3}?((a|A)ction|(a|A)ccent|(a|A)ctive|(a|A)ccident|A |C |sea|see)'
                            ,'g'
                            )
            from tmp.scanner_addresses
            UNION
            select distinct call_id
                ,main_transcript_cleaned
                ,regexp_matches(main_transcript_cleaned
                            ,'((accident|active|action|accent)\D{0,15}?(injur|andrew|andrea))|(roll.{0,1}?over)|vehicle (accident|active|action|accent)|motor relax|overturned vehicle'
                            ,'ig'
                            )
            from tmp.scanner_addresses
            UNION 
            select distinct call_id
            ,main_transcript_cleaned
            ,regexp_matches(main_transcript_cleaned
                        ,'(car|vehicle|truck).{0,5}?into a.{0,5}?(pole|tree|building)'
                        ,'ig'
                        )
            from tmp.scanner_addresses
        ) ;

        drop table if exists tmp.flag_motorcycles;
        create table tmp.flag_motorcycles as (
            select distinct call_id
            ,main_transcript_cleaned
            ,regexp_matches(main_transcript_cleaned
                        ,'motor.{0,1}?cycl'
                        ,'ig'
                        )
            from tmp.scanner_addresses
        ) ;

        drop table if exists tmp.flag_not_pedestrians;
        create table tmp.flag_not_pedestrians as (
            --struck by train
            select distinct call_id
            ,main_transcript_cleaned
            ,regexp_matches(main_transcript_cleaned
                        ,'(struck by.{0,3}?train)'
                        ,'ig'
                        )
            from tmp.scanner_addresses
        ) ;


        drop table if exists tmp.classify_calls;
        create table tmp.classify_calls as (
        select distinct a.*
            ,case when mva.call_id is not null or peds.call_id is not null or motorcycles.call_id is not null then 1 else 0 end as car_crash_call
            ,case when peds.call_id is not null and not_peds.call_id is null then 1 else 0 end as someone_outside_car_struck
            ,case when motorcycles.call_id is not null then 1 else 0 end as motorcycle_flag
        from tmp.scanner_addresses a
            left join tmp.flag_pedestrians peds on a.call_id = peds.call_id
            left join tmp.flag_accidents mva on a.call_id = mva.call_id
            left join tmp.flag_motorcycles motorcycles on a.call_id = motorcycles.call_id
            left join tmp.flag_not_pedestrians not_peds on not_peds.call_id = a.call_id
            );
            """

    final_query = """
    CREATE TABLE IF NOT EXISTS source_data.scanner_audio_classified (LIKE tmp.classify_calls);

    INSERT INTO source_data.scanner_audio_classified
        SELECT * FROM tmp.classify_calls;

    GRANT ALL PRIVILEGES ON source_data.scanner_audio_classified TO PUBLIC;
    """

    engine.execute(get_pulsepoint_info_query)
    print("unit numbers, addresses, and quadrants info extracted")
    engine.execute(link_pulsepoint_info_to_scanner_audio_query)
    print("unit numbers, addresses, and quadrants linked in")
    engine.execute(classify_scanner_calls_query)
    print("calls classified")
    engine.execute(final_query)
    print("final table source_data.scanner_audio_classified created")



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
    classify_scanner_audio(engine=engine)