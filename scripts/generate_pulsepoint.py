import sqlalchemy
from connect_to_rds import get_connection_strings, create_postgres_engine

dbname='postgres'
env="DEV"
engine = create_postgres_engine(destination="AWS_PostGIS", target_db=dbname, env=env)
db_credentials = get_connection_strings("AWS_PostGIS")

target_schema = 'source_data'
target_table='pulsepoint'

step1_query ="""
DROP TABLE IF EXISTS tmp_pulsepoint;
CREATE TEMP TABLE tmp_pulsepoint ON COMMIT PRESERVE ROWS 
AS ( 
SELECT 
    Incident_ID
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
        Incident_ID
        ,Scrape_Datetime
        ,CALL_RECEIVED_DATETIME
        ,CALL_Closed_DATETIME
        ,FullDisplayAddress
        ,longitude
        ,latitude
        ,Incident_Type
        ,Unit
        ,MAX(Unit_Status_Transport) over (Partition by Incident_ID) as Unit_Status_Transport
        ,MAX(Transport_Unit_Is_AMR) over (Partition by Incident_ID) as Transport_Unit_Is_AMR
        ,MAX(Transport_Unit_Is_Non_AMR) over (Partition by Incident_ID) as Transport_Unit_Is_Non_AMR
        ,ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography as geography
        ,cast(replace(unit,'''','"') as jsonb) as Unit_JSON
        ,JSONB_ARRAY_LENGTH(cast(replace(unit,'''','"') as jsonb)::jsonb) as Num_Units_Responding
        ,ROW_NUMBER() over (Partition by Incident_ID order by Scrape_Datetime DESC) as Time_Rank
        FROM source_data.pulsepoint_stream
    ) AS tmp
WHERE Time_Rank = 1
) WITH DATA;
"""

final_query="""
DROP TABLE IF EXISTS {0}.{1};

CREATE TABLE {0}.{1} AS 
    SELECT * FROM tmp_pulsepoint;

GRANT ALL PRIVILEGES ON {0}.{1} TO PUBLIC;
""".format(target_schema, target_table)

engine.execute(step1_query)
engine.execute(final_query)