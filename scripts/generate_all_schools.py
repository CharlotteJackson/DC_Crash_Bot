import sqlalchemy
from connect_to_rds import get_connection_strings, create_postgres_engine

dbname='postgres'
env="DEV"
engine = create_postgres_engine(destination="AWS_PostGIS", target_db=dbname, env=env)
db_credentials = get_connection_strings("AWS_PostGIS")

target_schema = 'analysis_data'
target_table='all_schools'

step1_query ="""
DROP TABLE IF EXISTS tmp_schools;
CREATE TEMP TABLE tmp_schools ON COMMIT PRESERVE ROWS 
AS ( 
    SELECT 
        0 as charter_school
        ,1 as public_school
        ,grades
        ,name as school_name 
        ,case when grades like \'PK3\%%\' or grades like \'PK4\%%\' or grades like \'1st\%%\' or grades like \'4th\%%\' or grades like \'\%%KG\%%\' then 1 else 0 end as ES 
        ,case when grades like \'6th\%%\' or grades like \'\%%8th\%%\' then 1 else 0 end as MS 
        ,case when grades like \'9th\%%\' or grades like \'\%%12th\%%\' or grades =\'Adult\' or grades =\'Alternative\' then 1 else 0 end as HS
        ,geography
    from source_data.public_schools
    UNION ALL
    SELECT 
        1 as charter_school
        ,0 as public_school
        ,grades
        ,name as school_name 
        ,case when grades like \'PK3\%%\' or grades like \'PK4\%%\' or grades like \'1st\%%\' or grades like \'4th\%%\' or grades like \'\%%KG\%%\' then 1 else 0 end as ES 
        ,case when grades like \'6th\%%\' or grades like \'\%%8th\%%\' then 1 else 0 end as MS 
        ,case when grades like \'9th\%%\' or grades like \'\%%12th\%%\' or grades =\'Adult\' or grades =\'Alternative\' then 1 else 0 end as HS
        ,geography
    from source_data.charter_schools
) WITH DATA;
"""

nbh_ward_query="""
CREATE TEMP TABLE tmp_schools_nbh_ward ON COMMIT PRESERVE ROWS 
AS (
	WITH anc_boundaries as (SELECT anc_id, ST_SUBDIVIDE(geography::geometry) geography FROM source_data.anc_boundaries),
		neighborhood_clusters as (SELECT name, nbh_names, ST_SUBDIVIDE(geography::geometry) geography FROM source_data.neighborhood_clusters),
		smd_boundaries as (SELECT smd_id, ST_SUBDIVIDE(geography::geometry) geography FROM source_data.smd_boundaries),
		ward_boundaries as (SELECT name, ST_SUBDIVIDE(geography::geometry) geography FROM source_data.ward_boundaries)
SELECT 
	c.anc_id
	,c.geography as anc_boundary
	,d.name as nbh_cluster
	,d.nbh_names as nbh_cluster_names
	,d.geography as nbh_cluster_boundary
	,e.smd_id
	,e.geography as smd_boundary
	,f.name as ward_name 
	,f.geography as ward_boundary
	,a.*
FROM tmp_schools a
LEFT JOIN anc_boundaries c ON ST_Intersects(c.geography::geometry, a.geography::geometry)
LEFT JOIN neighborhood_clusters d ON ST_Intersects(d.geography::geometry, a.geography::geometry)
LEFT JOIN smd_boundaries e ON ST_Intersects(e.geography::geometry, a.geography::geometry)
LEFT JOIN ward_boundaries f ON ST_Intersects(f.geography::geometry, a.geography::geometry)
) WITH DATA;
"""

final_query="""
DROP TABLE IF EXISTS {0}.{1};

CREATE TABLE {0}.{1} AS 
    SELECT * FROM tmp_schools_nbh_ward;

GRANT ALL PRIVILEGES ON {0}.{1} TO PUBLIC;
""".format(target_schema, target_table)

engine.execute(step1_query)
engine.execute(nbh_ward_query)
engine.execute(final_query)