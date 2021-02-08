from connect_to_rds import get_connection_strings, create_postgres_engine
from add_location_info import add_location_info, add_school_info, create_final_table

dbname='postgres'
env="DEV"
engine = create_postgres_engine(destination="AWS_PostGIS", target_db=dbname, env=env)
db_credentials = get_connection_strings("AWS_PostGIS")

step1_query ="""
DROP TABLE IF EXISTS tmp.acs_2019_by_tract;
CREATE TABLE tmp.acs_2019_by_tract
AS ( 
SELECT a.*
    ,(total_households - total_households_w_no_vehicle)/(total_households*1.00) as Pct_Households_W_Car
    ,(total_households - total_households_w_no_vehicle) as num_households_w_car
    ,b.fagi_median_2015
    ,b.fagi_total_2015
    ,b.geography
FROM source_data.acs_2019_by_tract  a
INNER JOIN source_data.census_tracts b on a.tract = b.tract
WHERE a.state = '11' and total_households>0

);

CREATE INDEX tmp_acs_2019_by_tract_index ON tmp.acs_2019_by_tract USING GIST (geography);
"""

# First execute the table-specific queries
engine.execute(step1_query)
print("step1 query complete")

# Then execute the same location-info queries (roadway, schools, neighborhoods) that apply to all analysis tables and create the final table
next_tables = add_location_info(engine=engine, target_schema='tmp', target_table='acs_2019_by_tract_nbh_ward', from_schema='tmp', from_table='acs_2019_by_tract', partition_by_field='tract')
print("neighborhood-ward query complete")
row_count = create_final_table(engine=engine, target_schema = 'analysis_data', target_table='acs_2019_by_tract', from_schema=next_tables[0], from_table=next_tables[1])
print("final query complete with row count ",row_count)

# specific to this dataset: assign some census tracts that contain large amts of parkland or water to the nearest populated neighborhood
engine.execute('update analysis_data.acs_2019_by_tract set nbh_cluster_names = \'Woodridge, Fort Lincoln, Gateway\', name = \'Cluster 24\' where census_tract = \'011100\';')
engine.execute('update analysis_data.acs_2019_by_tract set nbh_cluster_names = \'Takoma, Brightwood, Manor Park\', name = \'Cluster 17\' where census_tract = \'001803\';')
engine.execute('update analysis_data.acs_2019_by_tract set nbh_cluster_names = \'North Cleveland Park, Forest Hills, Van Ness\', name = \'Cluster 12\'  where census_tract = \'001301\';')