import sqlalchemy
from connect_to_rds import get_connection_strings, create_postgres_engine

dbname='postgres'
env="DEV"
engine = create_postgres_engine(destination="AWS_PostGIS", target_db=dbname, env=env)
db_credentials = get_connection_strings("AWS_PostGIS")

target_schema = 'analysis_data'
target_table='all311'

nbh_ward_query="""
DROP TABLE IF EXISTS tmp.all311_nbh_ward;
CREATE TABLE tmp.all311_nbh_ward 
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
    ,ROW_NUMBER() OVER (PARTITION BY a.objectid) as tsa_row_num
	,a.*
FROM source_data.all311 a
LEFT JOIN anc_boundaries c ON ST_Intersects(c.geography::geometry, a.geography::geometry)
LEFT JOIN neighborhood_clusters d ON ST_Intersects(d.geography::geometry, a.geography::geometry)
LEFT JOIN smd_boundaries e ON ST_Intersects(e.geography::geometry, a.geography::geometry)
LEFT JOIN ward_boundaries f ON ST_Intersects(f.geography::geometry, a.geography::geometry)
) 
"""

schools_query ="""
DROP TABLE IF EXISTS tmp.all311_schools;
CREATE  TABLE tmp.all311_schools 
AS (
    SELECT  ARRAY_AGG(distinct b.school_name) as near_schools
        , MAX(b.ES) as ES
        , MAX(b.MS) as MS
        , MAX(b.HS) as HS
        , MAX(b.public_school) as public_school
        , MAX(b.charter_school) as charter_school
        , a.* 
    FROM  tmp.all311_nbh_ward a
    LEFT JOIN analysis_data.all_schools b on ST_DWithin(b.geography,a.geography,200)
    WHERE a.tsa_row_num = 1
    GROUP BY 
        a.anc_id
        ,a.anc_boundary
        ,a.nbh_cluster
        ,a.nbh_cluster_names
        ,a.nbh_cluster_boundary
        ,a.smd_id
        ,a.smd_boundary
        ,a.ward_name
        ,a.ward_boundary
        ,a.objectid
        ,a.tsa_row_num
        ,SERVICECODE 
        ,SERVICECODEDESCRIPTION 
        ,SERVICETYPECODEDESCRIPTION 
        ,ORGANIZATIONACRONYM
        ,SERVICECALLCOUNT 
        ,ADDDATE 
        ,RESOLUTIONDATE 
        ,SERVICEDUEDATE 
        ,SERVICEORDERDATE
        ,INSPECTIONFLAG
        ,INSPECTIONDATE
        ,INSPECTORNAME
        ,SERVICEORDERSTATUS
        ,STATUS_CODE
        ,SERVICEREQUESTID
        ,PRIORITY
        ,STREETADDRESS
        ,XCOORD 
        ,YCOORD 
        ,LATITUDE 
        ,LONGITUDE 
        ,CITY
        ,STATE 
        ,ZIPCODE 
        ,MARADDRESSREPOSITORYID
        ,WARD 
        ,DETAILS
        ,a.geography
) 
"""
final_query="""
DROP TABLE IF EXISTS {0}.{1};

CREATE TABLE {0}.{1} AS 
    SELECT * FROM tmp.all311_schools;

GRANT ALL PRIVILEGES ON {0}.{1} TO PUBLIC;
""".format(target_schema, target_table)

engine.execute(nbh_ward_query)
engine.execute(schools_query)
engine.execute(final_query)