import sqlalchemy
from connect_to_rds import get_connection_strings, create_postgres_engine
from add_location_info import add_location_info
from add_school_info import add_school_info

dbname='postgres'
env="DEV"
engine = create_postgres_engine(destination="AWS_PostGIS", target_db=dbname, env=env)
db_credentials = get_connection_strings("AWS_PostGIS")

target_schema = 'analysis_data'
target_table='all311'

filter_query="""
DROP TABLE IF EXISTS tmp.all311_filtered;
CREATE TABLE tmp.all311_filtered 
AS (
SELECT *
FROM source_data.all311
WHERE servicecode in (\'MARKMAIN\', \'MARKMODI\', \'MARKINST\',   \'S0376\',\'SAROTOSC\', \'SCCRGUPR\', \'SPSTDAMA\')
) 
"""

final_query="""
DROP TABLE IF EXISTS {0}.{1};

CREATE TABLE {0}.{1} AS 
    SELECT * FROM tmp.all311_schools;

CREATE INDEX all311final_geom_idx ON {0}.{1} USING GIST (geography);
GRANT ALL PRIVILEGES ON {0}.{1} TO PUBLIC;
""".format(target_schema, target_table)

engine.execute(filter_query)
print("filter query executed")
add_location_info(engine=engine, target_schema='tmp', target_table='all311_nbh_ward', from_schema='tmp', from_table='all311_filtered', partition_by_field='objectid')
print("location info added")
add_school_info(engine=engine, target_schema='tmp', target_table='all311_schools', from_schema='tmp', from_table='all311_nbh_ward')
print("schools info added")
engine.execute(final_query)
print("final table created")