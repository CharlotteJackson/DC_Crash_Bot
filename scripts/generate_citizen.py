import sqlalchemy
from connect_to_rds import get_connection_strings, create_postgres_engine
import argparse

def generate_citizen (engine):

    step1_query = """
    DROP TABLE IF EXISTS source_data.citizen;
    CREATE TABLE source_data.citizen
    AS (
    SELECT DISTINCT
		MAX(ts) AS ts
		,MIN(cs) AS cs
		,REPLACE(incident_key,'"','') AS incident_key
		,REPLACE(incident_desc_raw,'"','') AS incident_desc_raw
		,REPLACE(incident_source,'"','') AS incident_source
		,CASE
			WHEN 
                (REPLACE(incident_desc_raw,'"','') ilike '%%pedestrian%%' 
                and REPLACE(incident_desc_raw,'"','') not ilike '%%pedestrian bridge%%'
                )
                OR (REPLACE(incident_desc_raw,'"','') ilike '%%struck by%%' 
                    AND REPLACE(incident_desc_raw,'"','') not ilike '%%gas line%%' 
                    AND REPLACE(incident_desc_raw,'"','') not ilike '%%gunfire%%'
                    )
				THEN 1 else 0 END as someone_outside_car_struck
		,CASE WHEN REPLACE(incident_desc_raw,'"','') ilike '%%motorcycl%%' THEN 1 ELSE 0 END AS motorcycle_flag
		,geography
	FROM source_data.citizen_stream 
	WHERE incident_categories::text ilike '%%traffic%%' AND (ts at time zone 'America/New_York')::date>='2021-05-15'
    GROUP BY 
        REPLACE(incident_key,'"','') 
		,REPLACE(incident_desc_raw,'"','') 
		,REPLACE(incident_source,'"','') 
		,CASE
			WHEN (REPLACE(incident_desc_raw,'"','') ilike '%%pedestrian%%' 
                and REPLACE(incident_desc_raw,'"','') not ilike '%%pedestrian bridge%%'
                )
                OR (REPLACE(incident_desc_raw,'"','') ilike '%%struck by%%' 
                    AND REPLACE(incident_desc_raw,'"','') not ilike '%%gas line%%' 
                    AND REPLACE(incident_desc_raw,'"','') not ilike '%%gunfire%%')
				THEN 1 else 0 END 
		,CASE WHEN REPLACE(incident_desc_raw,'"','') ilike '%%motorcycl%%' THEN 1 ELSE 0 END 
        ,geography
    );

    CREATE INDEX IF NOT EXISTS citizen_geom_idx ON source_data.citizen USING GIST (geography);

    GRANT ALL PRIVILEGES ON source_data.citizen TO PUBLIC;
   
    """

    # First execute the table-specific queries
    engine.execute(step1_query)
    print("step1 query complete")

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
    if env == None:
        env='DEV'
    env=env.upper()

    # set up RDS and S3 connections, engines, cursors
    engine = create_postgres_engine(destination="AWS_PostGIS", env=env)
    generate_citizen(engine)