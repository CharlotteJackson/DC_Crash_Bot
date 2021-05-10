import sqlalchemy
from connect_to_rds import get_connection_strings, create_postgres_engine
import argparse

def generate_pulsepoint_table (AWS_Credentials:dict, **kwargs):

    # assign optional arguments
    target_table='pulsepoint'
    target_schema=kwargs.get('target_schema', None)
    if target_schema == None:
        target_schema='source_data'
    target_table=kwargs.get('target_table', None)
    if target_table == None:
        target_table='pulsepoint'
    # if no environment is specified default to dev 
    env=kwargs.get('env', None)
    if env == None:
        env='DEV'
    env=env.upper()

    # set up RDS and S3 connections, engines, cursors
    region=AWS_Credentials['region']
    engine = create_postgres_engine(destination="AWS_PostGIS", env=env)


    step1_query ="""
    DROP TABLE IF EXISTS tmp_pulsepoint;
    CREATE TEMP TABLE tmp_pulsepoint ON COMMIT PRESERVE ROWS 
    AS ( 
    SELECT 
        Agency_ID
        ,Incident_ID
        ,Scrape_Datetime
        ,CALL_RECEIVED_DATETIME
        ,CALL_Closed_DATETIME
        ,FullDisplayAddress
        ,longitude
        ,latitude
        ,Incident_Type
        ,Unit
        ,Unit_Status_Transport
        ,Transport_Unit_Is_AMR
        ,Transport_Unit_Is_Non_AMR
        ,Unit_JSON
        ,Num_Units_Responding
        ,geography
    FROM (
        SELECT 
            Agency_ID
            ,Incident_ID
            ,Scrape_Datetime
            ,CALL_RECEIVED_DATETIME
            ,CALL_Closed_DATETIME
            ,FullDisplayAddress
            ,longitude
            ,latitude
            ,Incident_Type
            ,Unit
            ,MAX(Unit_Status_Transport) over (Partition by Agency_ID, Incident_ID) as Unit_Status_Transport
            ,MAX(Transport_Unit_Is_AMR) over (Partition by Agency_ID, Incident_ID) as Transport_Unit_Is_AMR
            ,MAX(Transport_Unit_Is_Non_AMR) over (Partition by Agency_ID, Incident_ID) as Transport_Unit_Is_Non_AMR
            ,ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography as geography
            ,cast(replace(unit,'''','"') as jsonb) as Unit_JSON
            ,JSONB_ARRAY_LENGTH(cast(replace(unit,'''','"') as jsonb)::jsonb) as Num_Units_Responding
            ,ROW_NUMBER() over (Partition by Agency_ID, Incident_ID order by Scrape_Datetime DESC) as Time_Rank
            FROM source_data.pulsepoint_stream
        ) AS tmp
    WHERE Time_Rank = 1
    ) WITH DATA;
    """

    step_2_query = """
    DROP TABLE IF EXISTS tmp_pulsepoint_units;
    CREATE TEMP TABLE tmp_pulsepoint_units ON COMMIT PRESERVE ROWS 
    AS (
        SELECT incident_id, Agency_ID, array_agg((Units#>'{{UnitID}}')::text) as Unit_IDs
        FROM tmp_pulsepoint
        CROSS JOIN json_array_elements(unit_json::json) as Units
        GROUP BY incident_id, Agency_ID
        ) WITH DATA; 
    """

    step_3_query = """
    DROP TABLE IF EXISTS tmp_pulsepoint_units_join;
    CREATE TEMP TABLE tmp_pulsepoint_units_join ON COMMIT PRESERVE ROWS 
    AS (
        SELECT DISTINCT a.*, b.Unit_IDs
        FROM tmp_pulsepoint a
        LEFT JOIN tmp_pulsepoint_units b on a.incident_id = b.incident_id and a.agency_id = b.agency_id
        ) WITH DATA; 
    """

    final_query="""
    DROP TABLE IF EXISTS {0}.{1};

    CREATE TABLE {0}.{1} AS 
        SELECT * FROM tmp_pulsepoint_units_join;

    GRANT ALL PRIVILEGES ON {0}.{1} TO PUBLIC;
    """.format(target_schema, target_table)

    engine.execute(step1_query)
    engine.execute(step_2_query)
    engine.execute(step_3_query)
    engine.execute(final_query)


# command line arguments
CLI=argparse.ArgumentParser()
CLI.add_argument(
"--env",
type=str
)

# parse the command line
args = CLI.parse_args()
env=args.env

if __name__ == "__main__":
    generate_pulsepoint_table(AWS_Credentials=get_connection_strings("AWS_DEV"), env=env)