from connect_to_rds import get_connection_strings, create_postgres_engine
from add_location_info import add_location_info
from add_school_info import add_school_info

dbname='postgres'
env="DEV"
engine = create_postgres_engine(destination="AWS_PostGIS", target_db=dbname, env=env)
db_credentials = get_connection_strings("AWS_PostGIS")

target_schema = 'analysis_data'
target_table='acs_2019_by_tract'

step1_query ="""
DROP TABLE IF EXISTS tmp.acs_2019_by_tract;
CREATE TABLE tmp.acs_2019_by_tract
AS ( 
SELECT a.*
    ,(total_households - total_households_w_no_vehicle)/(total_households*1.00) as Pct_Households_W_Car
    ,b.fagi_median_2015
    ,b.geography
FROM source_data.acs_2019_by_tract  a
INNER JOIN source_data.census_tracts b on a.tract = b.tract
WHERE a.state = '11' and total_households>0

);

CREATE INDEX tmp_acs_2019_by_tract_index ON tmp.acs_2019_by_tract USING GIST (geography);
"""

final_query="""
DROP TABLE IF EXISTS {0}.{1};

CREATE TABLE {0}.{1} AS 
    SELECT * FROM tmp.acs_2019_by_tract_schools;

GRANT ALL PRIVILEGES ON {0}.{1} TO PUBLIC;

CREATE INDEX {0}_{1}_index ON {0}.{1} USING GIST (geography);
""".format(target_schema, target_table)

engine.execute(step1_query)
print("initial query complete")
# add nearby schools
add_school_info(engine=engine, target_schema='tmp', target_table='acs_2019_by_tract_schools', from_schema='tmp', from_table='acs_2019_by_tract')
print("schools query complete")
engine.execute(final_query)
print("final query complete")