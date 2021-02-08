from connect_to_rds import get_connection_strings, create_postgres_engine
from add_location_info import add_location_info, add_school_info, create_final_table

dbname='postgres'
env="DEV"
engine = create_postgres_engine(destination="AWS_PostGIS", target_db=dbname, env=env)
db_credentials = get_connection_strings("AWS_PostGIS")

engine.execute('update source_data.census_blocks set geography = ST_MakeValid(geography::geometry)::geography')
print ('geography updated')

# Then execute the same location-info queries (roadway, schools, neighborhoods) that apply to all analysis tables and create the final table
next_tables = add_location_info(engine=engine, target_schema='tmp', target_table='census_blocks_nbh_ward', from_schema='source_data', from_table='census_blocks', partition_by_field='objectid')
print("neighborhood-ward query complete")
next_tables = add_school_info(engine=engine, target_schema='tmp', target_table='census_blocks_schools', from_schema=next_tables[0], from_table=next_tables[1])
print("schools query complete")
row_count = create_final_table(engine=engine, target_schema = 'analysis_data', target_table='census_blocks', from_schema=next_tables[0], from_table=next_tables[1])
print("final query complete with row count ",row_count)