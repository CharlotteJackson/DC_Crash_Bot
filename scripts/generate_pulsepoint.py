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

final_query=f"""
DROP TABLE IF EXISTS {target_schema}.{target_table};

CREATE TABLE {target_schema}.{target_table} AS 
    SELECT * FROM tmp_pulsepoint_units_join;

GRANT ALL PRIVILEGES ON {target_schema}.{target_table} TO PUBLIC;
"""

engine.execute(step1_query)
engine.execute(step_2_query)
engine.execute(step_3_query)
engine.execute(final_query)