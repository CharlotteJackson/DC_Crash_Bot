from connect_to_rds import get_connection_strings, create_postgres_engine
from json_to_postgis import json_to_postGIS
import argparse

def stg_to_source_data (source_table:str, target_table:str, engine, mode:str, **kwargs):

    # assign optional arguments
    source_schema=kwargs.get('source_schema', None)
    if source_schema == None:
        source_schema='stg'
    target_schema=kwargs.get('target_schema', None)
    if target_schema == None:
        target_schema='source_data'

    if mode.lower()=='truncate':
        final_query="""
        DROP TABLE IF EXISTS {target_schema}.{target_table};
        CREATE TABLE {target_schema}.{target_table}(LIKE {source_schema}."{source_table}");

        INSERT INTO {target_schema}.{target_table}
            SELECT * FROM {source_schema}."{source_table}";

        GRANT ALL PRIVILEGES ON {target_schema}.{target_table} TO PUBLIC;
        """.format(target_schema=target_schema, target_table=target_table, source_schema=source_schema, source_table=source_table)
    elif mode.lower()=='append':
        final_query="""
        INSERT INTO {target_schema}.{target_table}
            SELECT * FROM {source_schema}."{source_table}";
        """.format(target_schema=target_schema, target_table=target_table, source_schema=source_schema, source_table=source_table)
    elif mode.lower()=='replace':
        final_query="""
        DELETE FROM {target_schema}.{target_table} WHERE source_file IN (SELECT DISTINCT source_file FROM {source_schema}."{source_table}");
        INSERT INTO {target_schema}.{target_table}
            SELECT * FROM {source_schema}."{source_table}";
        """.format(target_schema=target_schema, target_table=target_table, source_schema=source_schema, source_table=source_table)


    engine.execute(final_query)

    drop_table_query = 'DROP TABLE IF EXISTS {}."{}"'.format(source_schema, source_table)
    engine.execute(drop_table_query)


CLI=argparse.ArgumentParser()
CLI.add_argument(
"source_table",
type=str
)
CLI.add_argument(
"target_table",
type=str
)
CLI.add_argument(
"mode",
type=str
)
CLI.add_argument(
"--env",
type=str
)
print(CLI)

# parse the command line
args = CLI.parse_args()
print(args)
env=args.env
source_table=args.source_table
target_table=args.target_table
mode=args.mode

if __name__ == "__main__":
    if env == None:
        env = 'DEV'
    env = env.upper()
    # tables_to_extract = json_to_postGIS(folder_to_load='source-data/citizen/unparsed/', move_to_folder = 'source-data/citizen/loaded_to_postgis/', AWS_Credentials=get_connection_strings("AWS_DEV"))
    engine = create_postgres_engine(destination="AWS_PostGIS", env=env)
    stg_to_source_data(source_table=source_table, target_table=target_table, engine=engine, mode=mode)