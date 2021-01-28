import sqlalchemy
from connect_to_rds import get_connection_strings, create_postgres_engine
from add_location_info import add_location_info, add_school_info, add_roadway_info, create_final_table

dbname='postgres'
env="DEV"
engine = create_postgres_engine(destination="AWS_PostGIS", target_db=dbname, env=env)
db_credentials = get_connection_strings("AWS_PostGIS")

step1_query ="""
DROP TABLE IF EXISTS tmp.all_schools;
CREATE TABLE tmp.all_schools 
AS ( 
    SELECT 
        0 as charter_school
        ,1 as public_school
        ,grades
        ,name as school_name 
        ,case when left(grades,3) in('PK3','PK4', '1st', '4th') then 1 else 0 end as ES 
        ,case when left(grades,3) in ('6th', '7th', '8th') then 1 else 0 end as MS 
        ,case when left(grades,3) in ('9th','10th','11th','12th') or grades ='Adult' or grades ='Alternative' then 1 else 0 end as HS
        ,geography
    from source_data.public_schools
    UNION ALL
    SELECT 
        1 as charter_school
        ,0 as public_school
        ,grades
        ,name as school_name 
        ,case when left(grades,3) in('PK3','PK4', '1st', '4th') then 1 else 0 end as ES 
        ,case when left(grades,3) in ('6th', '7th', '8th') then 1 else 0 end as MS 
        ,case when left(grades,3) in ('9th','10th','11th','12th') or grades ='Adult' or grades ='Alternative' then 1 else 0 end as HS
        ,geography
    from source_data.charter_schools
) ;
"""

# First execute the table-specific queries
engine.execute(step1_query)
print("step1 query complete")

# Then execute the same location-info queries (roadway, schools, neighborhoods) that apply to all analysis tables and create the final table
next_tables = add_location_info(engine=engine, target_schema='tmp', target_table='all_schools_nbh_ward', from_schema='tmp', from_table='all_schools', partition_by_field='school_name')
print("neighborhood-ward query complete")
row_count = create_final_table(engine=engine, target_schema = 'analysis_data', target_table='all_schools', from_schema=next_tables[0], from_table=next_tables[1])
print("final query complete with row count ",row_count)