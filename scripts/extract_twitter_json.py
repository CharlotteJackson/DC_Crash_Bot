from connect_to_rds import get_connection_strings, create_postgres_engine
from add_location_info import geocode_text
from json_to_postgis import json_to_postGIS
import argparse

def extract_twitter_json (target_schema:str, source_table:str, target_table:str, AWS_Credentials:dict, **kwargs):

    # assign optional arguments
    source_schema=kwargs.get('source_schema', None)
    if source_schema == None:
        source_schema='stg'
    # if no environment is specified default to dev 
    env=kwargs.get('env', None)
    if env == None:
        env='DEV'
    env=env.upper()

    # set up RDS and S3 connections, engines, cursors
    region=AWS_Credentials['region']
    engine = create_postgres_engine(destination="AWS_PostGIS", env=env)

    # extract the json info
    step1_query ="""
    DROP TABLE IF EXISTS tmp.twitter;
    CREATE TABLE tmp.twitter
    AS ( 
    WITH results AS (
        SELECT jsonb_array_elements(test.tweets) AS data, source_file, load_datetime
            FROM
                (
                SELECT data->'data' as tweets, source_file, load_datetime
                from {}."{}"
                ) as test
	)
	SELECT 
            (data->'created_at')::varchar::timestamptz AS created_at
            ,(data->'id_str')::varchar AS tweet_id
			,(data->'user'->'id_str')::varchar AS user_id
			,CASE WHEN (data ? 'retweeted_status') THEN (data->'retweeted_status'->'id_str')::varchar END AS retweeted_status_id
			,CASE WHEN(data->'in_reply_to_status_id_str')::varchar <> 'null'
				THEN (data->'in_reply_to_status_id_str')::varchar END AS in_reply_to_status_id
			,REPLACE(REPLACE(CASE WHEN (data ? 'full_text') THEN (data->'full_text')::varchar 
				WHEN (data ? 'text') THEN (data->'text')::varchar 
				END, '&amp;', '&'),'%%','percent') AS tweet_text
            ,source_file
            ,load_datetime
            ,data 
	    FROM results
        );
    """.format(source_schema, source_table)

    engine.execute(step1_query)

    # geocode records
    records = [r for (r,) in engine.execute("select distinct tweet_text from tmp.twitter").fetchall()]
    print(len(records)," records passed to geocode function")
    geocode_text(engine=engine, records_to_geocode = records, administrative_area='District of Columbia', text_type = 'Tweet')

    # join the geocoded text back into the main table
    step_2_query = """
    DROP TABLE IF EXISTS tmp.twitter_geocode;
    CREATE  TABLE tmp.twitter_geocode
    AS (
        SELECT DISTINCT a.*
            ,b.point_type
            ,b.point_geography
            ,b.polygon_geography
        FROM tmp.twitter a
        LEFT JOIN source_data.geocoded_text b on a.tweet_text = b.text
        ) ; 
    """

    final_query="""
    CREATE TABLE IF NOT EXISTS {0}.{1} (LIKE tmp.twitter_geocode);

    INSERT INTO {0}.{1} 
        SELECT * FROM tmp.twitter_geocode;

    GRANT ALL PRIVILEGES ON {0}.{1} TO PUBLIC;
    """.format(target_schema, target_table)

    engine.execute(step1_query)
    engine.execute(step_2_query)
    engine.execute(final_query)

    count_query = 'SELECT COUNT(*) FROM {}.{} WHERE source_file like \'%%{}%%\''.format(target_schema, target_table,source_table)
    
    row_count = engine.execute(count_query).fetchone()[0]
    print("{} rows inserted into final table from file {}".format(str(row_count), source_table))

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
    tables_to_extract = [r for (r,) in engine.execute("select distinct table_name from information_schema.tables where table_schema = 'stg' and table_name like '%%twitter%%'")]
    for table in tables_to_extract:
        extract_twitter_json(source_table=table, target_table='twitter_stream'
        , target_schema='source_data',AWS_Credentials=get_connection_strings("AWS_DEV"), env=env)