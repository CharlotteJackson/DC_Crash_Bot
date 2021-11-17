from connect_to_rds import get_connection_strings, create_postgres_engine
from json_to_postgis import json_to_postGIS
import argparse

def extract_waze_users_json (target_schema:str, source_table:str, target_table:str, engine, **kwargs):

    # assign optional arguments
    source_schema=kwargs.get('source_schema', None)
    if source_schema == None:
        source_schema='stg'

    # extract the json info
    users_query ="""
    DROP TABLE IF EXISTS tmp.waze_users_stream;
    CREATE TABLE tmp.waze_users_stream
    AS ( 
        
		WITH results AS 
            (
            SELECT jsonb_array_elements(user_list.users) AS users, source_file, scrape_datetime::varchar as scrape_datetime
            FROM
                (
                SELECT data->'users' as users, source_file, data->'scrape_datetime' as scrape_datetime
                from {source_schema}."{source_table}"
                ) as user_list
            ) 
        SELECT 
            (users->'id')::varchar as user_id
            ,(users->'mood')::varchar as user_mood
			,(users->'ping')::varchar as user_ping
			,(users->'addon')::varchar as user_addon
			,(users->'fleet')::varchar as user_fleet
			,(users->'speed')::numeric as user_speed
			,(users->'magvar')::varchar as user_magvar
			,(users->'ingroup')::varchar as user_ingroup
			,(users->'inscale')::varchar as user_inscale
			,(users->'location'->'x')::numeric as user_long
			,(users->'location'->'y')::numeric as user_lat
			,ST_SetSRID(ST_MakePoint((users->'location'->'x')::numeric, (users->'location'->'y')::numeric),4326)::geography as geography
			,(users->'userName')::varchar as user_username
            ,source_file
            ,scrape_datetime::timestamptz as scrape_datetime
            ,users as data 
	    FROM results
        );
    """.format(source_schema=source_schema, source_table=source_table)

    users_final_query="""
    CREATE TABLE IF NOT EXISTS {0}.{1} (LIKE tmp.waze_users_stream);

    INSERT INTO {0}.{1} 
        SELECT * FROM tmp.waze_users_stream;

    GRANT ALL PRIVILEGES ON {0}.{1} TO PUBLIC;
    """.format(target_schema, target_table)

    engine.execute(users_query)
    engine.execute(users_final_query)

    # count_query = 'SELECT COUNT(*) FROM {}.{} WHERE source_file like \'%%{}%%\''.format(target_schema, target_table,source_table)
    
    # row_count = engine.execute(count_query).fetchone()[0]
    # print("{} rows inserted into final table from file {}".format(str(row_count), source_table))


def extract_waze_alerts_json (target_schema:str, source_table:str, target_table:str, engine, **kwargs):

    # assign optional arguments
    source_schema=kwargs.get('source_schema', None)
    if source_schema == None:
        source_schema='stg'

    # extract the json info
    alerts_query ="""
    DROP TABLE IF EXISTS tmp.waze_alerts;
    CREATE TABLE tmp.waze_alerts
    AS ( 
        WITH results AS 
            (
            SELECT jsonb_array_elements(alerts_list.alerts) AS alerts, source_file, scrape_datetime::varchar as scrape_datetime
            FROM
                (
                SELECT data->'alerts' as alerts, source_file, data->'scrape_datetime' as scrape_datetime
                from {source_schema}."{source_table}"
                ) as alerts_list
            ) 
        SELECT 
            (alerts->'id')::varchar as alert_id
			,(alerts->'uuid')::varchar as alert_uuid
			,(alerts->'location'->'x')::numeric as alert_long
			,(alerts->'location'->'y')::numeric as alert_lat
			,ST_SetSRID(ST_MakePoint((alerts->'location'->'x')::numeric, (alerts->'location'->'y')::numeric),4326)::geography as geography
            ,(alerts->'city')::varchar as alert_city
			,(alerts->'type')::varchar as alert_type
			,(alerts->'speed')::varchar as alert_speed
			,(alerts->'magvar')::varchar as alert_magvar
			,(alerts->'street')::varchar as alert_street
			,(alerts->'country')::varchar as alert_country
			,(alerts->'inscale')::varchar as alert_inscale
			,(alerts->'nImages')::numeric as alert_nImages
			,(alerts->'comments') as alert_comments
			,(alerts->'nComments')::numeric as alert_nComments
			,(alerts->'nThumbsUp')::numeric as alert_nThumbsUp
			,(alerts->'roadType')::varchar as alert_roadtype
			,(alerts->'wazeData') as alert_wazeData
			,(alerts->'confidence')::numeric as alert_confidence
			,(alerts->'reportMood')::numeric as alert_reportMood
			,(alerts->'reliability')::numeric as alert_reliability
			,(alerts->'reportDescription')::varchar as alert_reportDescription
			,(alerts->'additionalInfo')::varchar as alert_additionalInfo
			,(alerts->'reportRating')::varchar as alert_reportRating
			,to_timestamp(TRUNC((alerts->'pubMillis')::bigint)/1000)::timestamptz AS pub_datetime
			,source_file
            ,scrape_datetime::timestamptz as scrape_datetime
			,alerts as data
            ,(alerts->'subtype')::varchar as alert_subtype
	    FROM results	
        );
    """.format(source_schema=source_schema, source_table=source_table)

    alerts_final_query="""
    CREATE TABLE IF NOT EXISTS {0}.{1} (LIKE tmp.waze_alerts);

    INSERT INTO {0}.{1} 
        SELECT * FROM tmp.waze_alerts;

    GRANT ALL PRIVILEGES ON {0}.{1} TO PUBLIC;
    """.format(target_schema, target_table)

    engine.execute(alerts_query)
    engine.execute(alerts_final_query)

    # count_query = 'SELECT COUNT(*) FROM {}.{} WHERE source_file like \'%%{}%%\''.format(target_schema, target_table,source_table)
    
    # row_count = engine.execute(count_query).fetchone()[0]
    # print("{} rows inserted into final table from file {}".format(str(row_count), source_table))

def extract_waze_jams_json (target_schema:str, source_table:str, target_table:str, engine, **kwargs):

    # assign optional arguments
    source_schema=kwargs.get('source_schema', None)
    if source_schema == None:
        source_schema='stg'

    # extract the line geometry info
    jams_query ="""
    DROP TABLE IF EXISTS tmp.waze_jams;
    CREATE TABLE tmp.waze_jams 
    AS ( 

		WITH results AS 
            (
            SELECT jsonb_array_elements(jams_list.jams) AS jams, source_file, scrape_datetime::varchar as scrape_datetime
            FROM
                (
                SELECT data->'jams' as jams, source_file, data->'scrape_datetime' as scrape_datetime
                from {source_schema}."{source_table}"
                ) as jams_list
            ) 
        SELECT 
            (jams->'id')::varchar as jam_id
			,(jams->'uuid')::varchar as jam_uuid
            ,(jams->'city')::varchar as jam_city
			,(jams->'type')::varchar as jam_type
			,(jams->'speed')::numeric as jam_speed
			,(jams->'delay')::int as jam_delay
			,(jams->'level')::int as jam_level
			,(jams->'length')::int as jam_length
			,(jams->'street')::varchar as jam_street
			,(jams->'country')::varchar as jam_country
			,(jams->'roadType')::int as jam_roadType
			,(jams->'segments') as jam_segments
			,(jams->'severity')::varchar as jam_severity
			,(jams->'endNode')::varchar as jam_endNode
			,(jams->'speedKMH')::numeric as jam_speedKMH
			,(jams->'turnType')::varchar as jam_turnType
			,(jams->'blockType')::varchar as jam_blockType
			,to_timestamp(TRUNC((jams->'blockUpdate')::bigint)/1000)::timestamptz as jam_blockUpdate
			,to_timestamp(TRUNC((jams->'blockStartTime')::bigint)/1000)::timestamptz as jam_blockStartTime
			,to_timestamp(TRUNC((jams->'blockExpiration')::bigint)/1000)::timestamptz  as jam_blockExpiration
			,(jams->'blockingAlertID')::varchar as jam_blockingAlertID
			,(jams->'blockDescription')::varchar as jam_blockDescription
			,(jams->'blockingAlertUuid')::varchar as jam_blockingAlertUuid
			,to_timestamp(TRUNC((jams->'pubMillis')::bigint)/1000)::timestamptz AS pub_datetime
			,to_timestamp(TRUNC((jams->'updateMillis')::bigint)/1000)::timestamptz AS update_datetime
			,source_file
            ,scrape_datetime::timestamptz as scrape_datetime
			,jams as data
	    FROM results		
        );
    """.format(source_schema=source_schema, source_table=source_table)

    jams_geo_query ="""
    DROP TABLE IF EXISTS tmp.waze_jams_points;
	CREATE TABLE tmp.waze_jams_points AS (
	WITH results AS 
            (
            SELECT jsonb_array_elements(jams_list.jams) AS jams
            FROM
                (
                SELECT data->'jams' as jams
                from {source_schema}."{source_table}"
                ) as jams_list
            ) 
		SELECT (jams->'uuid')::varchar as jam_uuid, a.*
		FROM results, jsonb_array_elements(jams->'line') with ordinality as a
        );
    """.format(source_schema=source_schema, source_table=source_table)

    jams_join_query ="""
    DROP TABLE IF EXISTS tmp.waze_jams_geo;
    CREATE TABLE tmp.waze_jams_geo 
    AS ( 
        
		WITH geo AS 
            (
            SELECT jam_uuid, ST_MakeLine(ST_SetSRID(ST_MakePoint((a.value->'x')::numeric, (a.value->'y')::numeric),4326) ORDER BY a.ordinality) AS geography
	        FROM tmp.waze_jams_points a
	        GROUP BY jam_uuid
            ) 
        SELECT DISTINCT a.*, b.geography
	    FROM tmp.waze_jams a
        INNER JOIN geo b on a.jam_uuid = b.jam_uuid
        );
    """.format(source_schema=source_schema, source_table=source_table)

    jams_final_query="""
    CREATE TABLE IF NOT EXISTS {0}.{1} (LIKE tmp.waze_jams_geo);

    INSERT INTO {0}.{1} 
        SELECT * FROM tmp.waze_jams_geo;

    GRANT ALL PRIVILEGES ON {0}.{1} TO PUBLIC;
    """.format(target_schema, target_table)

    engine.execute(jams_query)
    engine.execute(jams_geo_query)
    engine.execute(jams_join_query)
    engine.execute(jams_final_query)

    # count_query = 'SELECT COUNT(*) FROM {}.{} WHERE source_file like \'%%{}%%\''.format(target_schema, target_table,source_table)
    
    # row_count = engine.execute(count_query).fetchone()[0]
    # print("{} rows inserted into final table from file {}".format(str(row_count), source_table))

    drop_table_query = 'DROP TABLE IF EXISTS {}."{}"'.format(source_schema, source_table)
    engine.execute(drop_table_query)

CLI=argparse.ArgumentParser()
CLI.add_argument(
"--env",
type=str
)
CLI.add_argument(
"--source_schema",
type=str
)

# parse the command line
args = CLI.parse_args()
env=args.env
source_schema=args.source_schema

if __name__ == "__main__":
    if env == None:
        env = 'DEV'
    env = env.upper()
    # tables_to_extract = json_to_postGIS(folder_to_load='source-data/citizen/unparsed/', move_to_folder = 'source-data/citizen/loaded_to_postgis/', AWS_Credentials=get_connection_strings("AWS_DEV"))
    engine = create_postgres_engine(destination="AWS_PostGIS", env=env)
    tables_to_extract = [r for (r,) in engine.execute("select distinct table_name from information_schema.tables where table_schema = 'stg' and table_name like '%%waze%%'")]
    for table in tables_to_extract:
        extract_waze_users_json(source_table=table, target_table='waze_users_stream', target_schema='source_data',engine=engine)
        extract_waze_alerts_json(source_table=table, target_table='waze_alerts_stream', target_schema='source_data',engine=engine)
        extract_waze_jams_json(source_table=table, target_table='waze_jams_stream', target_schema='source_data',engine=engine)