from connect_to_rds import get_connection_strings, create_postgres_engine
from json_to_postgis import json_to_postGIS
import argparse

def extract_scanner_audio_json (target_schema:str, source_table:str, target_table:str, AWS_Credentials:dict, **kwargs):

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
    DROP TABLE IF EXISTS tmp.citizen;
    CREATE TABLE tmp.citizen
    AS ( 
        WITH results AS 
            (
            SELECT jsonb_array_elements(test.results) AS data, source_file, load_datetime
            FROM
                (
                SELECT data->'results' as results, source_file, load_datetime
                from {0}."{1}"
                ) as test
            ) 
        SELECT 
            to_timestamp(TRUNC((data->'cs')::bigint)/1000) AS cs
            ,(data->'ll'->0)::numeric AS lat
            ,(data->'ll'->1)::numeric AS long
            ,to_timestamp(TRUNC((data->'ts')::bigint)/1000) AS ts
            ,(data->'key')::varchar as incident_key
            ,(data->'raw')::varchar as incident_desc_raw
            ,(data->'source')::varchar as incident_source
            ,(data->'categories') as incident_categories
            ,source_file
            ,load_datetime
            ,data 
	    FROM results
        );
    """.format(source_schema, source_table)

    step_2_query = """
    DROP TABLE IF EXISTS tmp.citizen_geometry;
    CREATE  TABLE tmp.citizen_geometry
    AS (
        SELECT *, ST_SetSRID(ST_MakePoint(long, lat),4326)::geography as geography
        FROM tmp.citizen
        ) ; 
    """

    final_query="""
    CREATE TABLE IF NOT EXISTS {0}.{1} (LIKE tmp.citizen_geometry);

    INSERT INTO {0}.{1} 
        SELECT * FROM tmp.citizen_geometry;

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
    tables_to_extract = [r for (r,) in engine.execute("select distinct table_name from information_schema.tables where table_schema = 'stg' and table_name like '%%transcribed_audio%%'")]
    for table in tables_to_extract:
        extract_citizen_json(source_table=table, target_table='citizen_stream'
        , target_schema='source_data',AWS_Credentials=get_connection_strings("AWS_DEV"), env=env)