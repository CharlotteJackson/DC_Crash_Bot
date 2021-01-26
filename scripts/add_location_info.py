import sqlalchemy
from connect_to_rds import get_connection_strings, create_postgres_engine

dbname='postgres'
env="DEV"
engine = create_postgres_engine(destination="AWS_PostGIS", target_db=dbname, env=env)
db_credentials = get_connection_strings("AWS_PostGIS")

def add_location_info(engine, from_schema:str, from_table:str, target_schema:str, target_table:str, partition_by_field:str):

    point_location_query="""
        DROP TABLE IF EXISTS {0}.{1};
        CREATE TABLE {0}.{1}
        AS (

        SELECT 
            c.anc_id
            ,d.name as nbh_cluster
            ,d.nbh_names as nbh_cluster_names
            ,e.smd_id
            ,f.name as ward_name 
            ,g.tract as census_tract
            ,h.name as comp_plan_area 
            ,ROW_NUMBER() OVER (PARTITION BY a.{5}) as row_num
            ,a.*
        FROM {2}.{3} a
        LEFT JOIN tmp.anc_boundaries c ON ST_Intersects(c.geography::geometry, a.geography::geometry)
        LEFT JOIN tmp.neighborhood_clusters d ON ST_Intersects(d.geography::geometry, a.geography::geometry)
        LEFT JOIN tmp.smd_boundaries e ON ST_Intersects(e.geography::geometry, a.geography::geometry)
        LEFT JOIN tmp.ward_boundaries f ON ST_Intersects(f.geography::geometry, a.geography::geometry)
        LEFT JOIN source_data.census_tracts g ON ST_Intersects(g.geography::geometry, a.geography::geometry)
        LEFT JOIN source_data.comp_plan_areas h ON ST_Intersects(h.geography::geometry, a.geography::geometry)
        ) ;

        SELECT COUNT(*) FROM {0}.{1};

        DELETE FROM {0}.{1} WHERE row_num >1;

        ALTER TABLE {0}.{1} DROP COLUMN row_num;

        SELECT COUNT(*) FROM {0}.{1};

        CREATE INDEX {4} ON {0}.{1} USING GIST (geography);
    """.format(target_schema, target_table, from_schema, from_table, target_schema+'_'+target_table+'_index',partition_by_field)

    non_point_location_query="""
        DROP TABLE IF EXISTS {0}.{1};
        CREATE TABLE {0}.{1}
        AS (

        SELECT 
            c.anc_id
            ,ROW_NUM() OVER (PARTITION BY a.{5} order by ST_Length(ST_Intersection(c.geography::geometry, a.geography::geometry)) desc) as ANC_Rank 
            ,d.name as nbh_cluster
            ,d.nbh_names as nbh_cluster_names
            ,ROW_NUM() OVER (PARTITION BY a.{5} order by ST_Length(ST_Intersection(d.geography::geometry, a.geography::geometry)) desc) as NBH_Rank 
            ,e.smd_id
            ,ROW_NUM() OVER (PARTITION BY a.{5} order by ST_Length(ST_Intersection(e.geography::geometry, a.geography::geometry)) desc) as SMD_Rank 
            ,f.name as ward_name 
            ,ROW_NUM() OVER (PARTITION BY a.{5} order by ST_Length(ST_Intersection(f.geography::geometry, a.geography::geometry)) desc) as Ward_Rank 
            ,g.tract as census_tract
            ,ROW_NUM() OVER (PARTITION BY a.{5} order by ST_Length(ST_Intersection(g.geography::geometry, a.geography::geometry)) desc) as Tract_Rank 
            ,h.name as comp_plan_area 
            ,ROW_NUM() OVER (PARTITION BY a.{5} order by ST_Length(ST_Intersection(h.geography::geometry, a.geography::geometry)) desc) as CompPlan_Rank 
            ,a.*
        FROM {2}.{3} a
        LEFT JOIN tmp.anc_boundaries c ON ST_Intersects(c.geography::geometry, a.geography::geometry)
        LEFT JOIN tmp.neighborhood_clusters d ON ST_Intersects(d.geography::geometry, a.geography::geometry)
        LEFT JOIN tmp.smd_boundaries e ON ST_Intersects(e.geography::geometry, a.geography::geometry)
        LEFT JOIN tmp.ward_boundaries f ON ST_Intersects(f.geography::geometry, a.geography::geometry)
        LEFT JOIN source_data.census_tracts g ON ST_Intersects(g.geography::geometry, a.geography::geometry)
        LEFT JOIN source_data.comp_plan_areas h ON ST_Intersects(h.geography::geometry, a.geography::geometry)
        ) ;

        SELECT COUNT(*) FROM {0}.{1};

        DELETE FROM {0}.{1} WHERE (ANC_Rank > 1 or NBH_Rank > 1  or SMD_Rank > 1  or Ward_Rank > 1  or Tract_Rank > 1  or CompPlan_Rank > 1);

        SELECT COUNT(*) FROM {0}.{1};

        ALTER TABLE {0}.{1} DROP COLUMN ANC_Rank;
        ALTER TABLE {0}.{1} DROP COLUMN NBH_Rank;
        ALTER TABLE {0}.{1} DROP COLUMN SMD_Rank;
        ALTER TABLE {0}.{1} DROP COLUMN Ward_Rank;
        ALTER TABLE {0}.{1} DROP COLUMN Tract_Rank;
        ALTER TABLE {0}.{1} DROP COLUMN CompPlan_Rank;

        CREATE INDEX {4} ON {0}.{1} USING GIST (geography);
    """.format(target_schema, target_table, from_schema, from_table, target_schema+'_'+target_table+'_index',partition_by_field)

    # check whether the target table has a geography field
    check_geo_field_type_query = """
    SELECT ST_GeometryType(geography::geometry) from {0}.{1} LIMIT 1
    """.format(from_schema, from_table)

    # check to see if table has a geography field, if yes, make sure it's the right format and create an index
    geo_field_type = engine.execute(check_geo_field_type_query).fetchone()[0]
    print(geo_field_type)
    if 'ST_Point' in geo_field_type:
        print('executing point query')
        engine.execute(point_location_query)
    else:
        print('executing non-point query')
        engine.execute(non_point_location_query)