from connect_to_rds import get_connection_strings, create_postgres_engine
from json_to_postgis import json_to_postGIS
import argparse

def extract_scanner_audio_json (target_schema:str, source_table:str, target_table:str, engine, **kwargs):

    # assign optional arguments
    source_schema=kwargs.get('source_schema', None)
    if source_schema == None:
        source_schema='stg'

    # extract the json info
    step1_query ="""
    DROP TABLE IF EXISTS tmp.openmhz;
    CREATE TABLE tmp.openmhz
    AS ( 
        WITH results AS 
            (
            SELECT jsonb_array_elements(test.results) AS data, source_file, load_datetime
            FROM
                (
                SELECT data->'data' as results, source_file, load_datetime
                from {0}."{1}"
                ) as test
            ) 
        SELECT 
			(data->'timestamp')::varchar::timestamptz AS call_timestamp
            ,(data->'call_length')::numeric AS call_length
            ,(data->'source')::varchar AS call_talkgroup
            ,(data->'id')::varchar as call_id
			,(data->'transcribed_audio'->'transcripts'->0->'transcript')::varchar as main_transcript
            ,source_file
            ,load_datetime
            ,data 
	    FROM results
        );
    """.format(source_schema, source_table)

    final_query="""
    CREATE TABLE IF NOT EXISTS {0}.{1} (LIKE tmp.openmhz);

    INSERT INTO {0}.{1} 
        SELECT * FROM tmp.openmhz WHERE main_transcript IS NOT NULL;

    GRANT ALL PRIVILEGES ON {0}.{1} TO PUBLIC;
    """.format(target_schema, target_table)

    engine.execute(step1_query)
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
    tables_to_extract = [r for (r,) in engine.execute("select distinct table_name from information_schema.tables where table_schema = 'stg' and table_name like '%%transcribed_audio%%'")]
    for table in tables_to_extract:
        extract_scanner_audio_json(source_table=table, target_table='openmhz', target_schema='source_data',engine=engine)