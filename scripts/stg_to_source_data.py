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
        # get the list of column names from the target table
        get_target_columns_query = """
        SELECT DISTINCT column_name, ordinal_position
        FROM information_schema.columns 
        WHERE table_schema = '{target_schema}'
        AND table_name = '{target_table}'
        ORDER BY ordinal_position
        """.format(target_schema=target_schema, target_table=target_table)

        target_columns = [r.lower() for (r,p) in engine.execute(get_target_columns_query).fetchall()]
        
        # then get the list of column names from the source table 
        get_source_columns_query = """
        SELECT DISTINCT column_name, ordinal_position
        FROM information_schema.columns 
        WHERE table_schema = '{source_schema}'
        AND table_name = '{source_table}'
        ORDER BY ordinal_position
        """.format(source_schema=source_schema, source_table=source_table)

        source_columns = [r.lower() for (r,p) in engine.execute(get_source_columns_query).fetchall()]

        # then get the intersection of those columns and turn it into a string
        overlapping_columns = list(sorted(set(source_columns) & set(target_columns), key=target_columns.index))
        overlapping_columns_string = overlapping_columns[0]
        for column in overlapping_columns[1:]:
            overlapping_columns_string+=' ,'
            overlapping_columns_string+=column
        print(overlapping_columns_string)

        # then select only those columns into the target table
        final_query="""
        INSERT INTO {target_schema}.{target_table}
            SELECT {overlapping_columns_string} FROM {source_schema}."{source_table}";
        """.format(target_schema=target_schema, target_table=target_table, source_schema=source_schema, source_table=source_table, overlapping_columns_string = overlapping_columns_string)
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