import sqlalchemy
from connect_to_rds import get_connection_strings, create_postgres_engine

def add_location_info(engine, from_schema:str, from_table:str, target_schema:str, target_table:str, partition_by_field:str):

    # check whether the target table has a geography field
    check_geo_field_type_query = """
    SELECT ST_GeometryType(geography::geometry) from {0}.{1} WHERE geography IS NOT NULL LIMIT 1
    """.format(from_schema, from_table)

    # depending on whether the geo type is a line, point, or polygon, execute appropriate query
    geo_field_type = engine.execute(check_geo_field_type_query).fetchone()[0]
    print(geo_field_type)
    if 'ST_Point' in geo_field_type:
    # build the query for point-level comparisons
        point_location_query="""
            DROP TABLE IF EXISTS anc_boundaries;
            CREATE TEMP TABLE anc_boundaries ON COMMIT PRESERVE ROWS AS (
                SELECT 
                c.anc_id
                ,ROW_NUMBER() OVER (PARTITION BY a.{5}) as ANC_Rank 
                ,a.{5} 
            FROM {2}.{3} a
            LEFT JOIN source_data.anc_boundaries c ON ST_Intersects(c.geography::geometry, a.geography::geometry)
            ) WITH DATA;
            
            DELETE FROM anc_boundaries WHERE ANC_Rank > 1;

            DROP TABLE IF EXISTS nbh_boundaries;
            CREATE TEMP TABLE nbh_boundaries ON COMMIT PRESERVE ROWS AS (
                SELECT 
                d.name as nbh_cluster
                ,d.nbh_names as nbh_cluster_names
                ,ROW_NUMBER() OVER (PARTITION BY a.{5}) as NBH_Rank 
                ,a.{5} 
            FROM {2}.{3} a
            LEfT JOIN source_data.neighborhood_clusters d ON ST_Intersects(d.geography::geometry, a.geography::geometry)
            ) WITH DATA;

            DELETE FROM nbh_boundaries WHERE NBH_Rank > 1;

            DROP TABLE IF EXISTS smd_boundaries;
            CREATE TEMP TABLE smd_boundaries ON COMMIT PRESERVE ROWS AS (
                SELECT 
                e.smd_id
                ,ROW_NUMBER() OVER (PARTITION BY a.{5}) as SMD_Rank 
                ,a.{5} 
            FROM {2}.{3} a
            LEFT JOIN source_data.smd_boundaries e ON ST_Intersects(e.geography::geometry, a.geography::geometry)
            ) WITH DATA;

            DELETE FROM smd_boundaries WHERE SMD_Rank >1;

            DROP TABLE IF EXISTS ward_boundaries;
            CREATE TEMP TABLE ward_boundaries ON COMMIT PRESERVE ROWS AS (
                SELECT 
                f.name as ward_name 
                ,ROW_NUMBER() OVER (PARTITION BY a.{5}) as Ward_Rank 
                ,a.{5} 
            FROM {2}.{3} a
            LEFT JOIN source_data.ward_boundaries f ON ST_Intersects(f.geography::geometry, a.geography::geometry)
            ) WITH DATA;

            DELETE FROM ward_boundaries WHERE Ward_Rank >1;

            DROP TABLE IF EXISTS census_tract_boundaries;
            CREATE TEMP TABLE census_tract_boundaries ON COMMIT PRESERVE ROWS AS (
                SELECT 
                g.tract as census_tract
                ,ROW_NUMBER() OVER (PARTITION BY a.{5}) as Tract_Rank 
                ,a.{5} 
            FROM {2}.{3} a
            LEFT JOIN source_data.census_tracts g ON ST_Intersects(g.geography::geometry, a.geography::geometry)
            ) WITH DATA;

            DELETE FROM census_tract_boundaries WHERE Tract_Rank >1;

            DROP TABLE IF EXISTS comp_plan_boundaries;
            CREATE TEMP TABLE comp_plan_boundaries ON COMMIT PRESERVE ROWS AS (
                SELECT 
                h.name as comp_plan_area 
                ,ROW_NUMBER() OVER (PARTITION BY a.{5}) as CompPlan_Rank 
                ,a.{5} 
            FROM {2}.{3} a
            LEFT JOIN source_data.comp_plan_areas h ON ST_Intersects(h.geography::geometry, a.geography::geometry)
            ) WITH DATA;

            DELETE FROM comp_plan_boundaries WHERE CompPlan_Rank >1;
            
            DROP TABLE IF EXISTS {0}.{1};
            CREATE TABLE {0}.{1}
            AS (

            SELECT DISTINCT
                b.anc_id
                ,c.nbh_cluster
                ,c.nbh_cluster_names
                ,d.smd_id
                ,e.ward_name
                ,f.census_tract
                ,g.comp_plan_area
                ,a.*
            FROM {2}.{3} a
            LEFT JOIN anc_boundaries b on a.{5} = b.{5}
            LEFT JOIN nbh_boundaries c on a.{5} = c.{5}
            LEFT JOIN smd_boundaries d on a.{5} = d.{5}
            LEFT JOIN ward_boundaries e on a.{5} = e.{5}
            LEFT JOIN census_tract_boundaries f on a.{5} = f.{5}
            LEFT JOIN comp_plan_boundaries g on a.{5} = g.{5}
            );

            CREATE INDEX {4} ON {0}.{1} USING GIST (geography);
        """.format(target_schema, target_table, from_schema, from_table, target_schema+'_'+target_table+'_index',partition_by_field)
    
    # execute the query for point-level comparisons
        engine.execute(point_location_query)    
    
    else:
        if 'linestring' in geo_field_type.lower():
            overlap_variable = 'ST_Length'
            join_type = 'LEFT'
        if 'polygon' in geo_field_type.lower():
            overlap_variable = 'ST_Area'
            join_type = 'INNER'


        print(overlap_variable)

        non_point_location_query="""
            
            DROP TABLE IF EXISTS anc_boundaries;
            CREATE TEMP TABLE anc_boundaries ON COMMIT PRESERVE ROWS AS (
                SELECT 
                c.anc_id
                ,ROW_NUMBER() OVER (PARTITION BY a.{5} order by {6}(ST_Intersection(c.geography::geometry, a.geography::geometry)::geometry) desc) as ANC_Rank 
                ,a.{5} 
            FROM {2}.{3} a
            {7} JOIN source_data.anc_boundaries c ON ST_Intersects(c.geography::geometry, a.geography::geometry)
            ) WITH DATA;
            
            DELETE FROM anc_boundaries WHERE ANC_Rank > 1;

            DROP TABLE IF EXISTS nbh_boundaries;
            CREATE TEMP TABLE nbh_boundaries ON COMMIT PRESERVE ROWS AS (
                SELECT 
                d.name as nbh_cluster
                ,d.nbh_names as nbh_cluster_names
                ,ROW_NUMBER() OVER (PARTITION BY a.{5} order by {6}(ST_Intersection(d.geography::geometry, a.geography::geometry)::geometry) desc) as NBH_Rank 
                ,a.{5} 
            FROM {2}.{3} a
            {7} JOIN source_data.neighborhood_clusters d ON ST_Intersects(d.geography::geometry, a.geography::geometry)
            ) WITH DATA;

            DELETE FROM nbh_boundaries WHERE NBH_Rank > 1;

            DROP TABLE IF EXISTS smd_boundaries;
            CREATE TEMP TABLE smd_boundaries ON COMMIT PRESERVE ROWS AS (
                SELECT 
                e.smd_id
                ,ROW_NUMBER() OVER (PARTITION BY a.{5} order by {6}(ST_Intersection(e.geography::geometry, a.geography::geometry)::geometry) desc) as SMD_Rank 
                ,a.{5} 
            FROM {2}.{3} a
            {7}  JOIN source_data.smd_boundaries e ON ST_Intersects(e.geography::geometry, a.geography::geometry)
            ) WITH DATA;

            DELETE FROM smd_boundaries WHERE SMD_Rank >1;

            DROP TABLE IF EXISTS ward_boundaries;
            CREATE TEMP TABLE ward_boundaries ON COMMIT PRESERVE ROWS AS (
                SELECT 
                f.name as ward_name 
                ,ROW_NUMBER() OVER (PARTITION BY a.{5} order by {6}(ST_Intersection(f.geography::geometry, a.geography::geometry)::geometry) desc) as Ward_Rank 
                ,a.{5} 
            FROM {2}.{3} a
            {7}  JOIN source_data.ward_boundaries f ON ST_Intersects(f.geography::geometry, a.geography::geometry)
            ) WITH DATA;

            DELETE FROM ward_boundaries WHERE Ward_Rank >1;

            DROP TABLE IF EXISTS census_tract_boundaries;
            CREATE TEMP TABLE census_tract_boundaries ON COMMIT PRESERVE ROWS AS (
                SELECT 
                g.tract as census_tract
                ,ROW_NUMBER() OVER (PARTITION BY a.{5} order by {6}(ST_Intersection(g.geography::geometry, a.geography::geometry)::geometry) desc) as Tract_Rank 
                ,a.{5} 
            FROM {2}.{3} a
            {7}  JOIN source_data.census_tracts g ON ST_Intersects(g.geography::geometry, a.geography::geometry)
            ) WITH DATA;

            DELETE FROM census_tract_boundaries WHERE Tract_Rank >1;

            DROP TABLE IF EXISTS comp_plan_boundaries;
            CREATE TEMP TABLE comp_plan_boundaries ON COMMIT PRESERVE ROWS AS (
                SELECT 
                h.name as comp_plan_area 
                ,ROW_NUMBER() OVER (PARTITION BY a.{5} order by {6}(ST_Intersection(h.geography::geometry, a.geography::geometry)::geometry) desc) as CompPlan_Rank 
                ,a.{5} 
            FROM {2}.{3} a
            {7}  JOIN source_data.comp_plan_areas h ON ST_Intersects(h.geography::geometry, a.geography::geometry)
            ) WITH DATA;

            DELETE FROM comp_plan_boundaries WHERE CompPlan_Rank >1;
            
            DROP TABLE IF EXISTS {0}.{1};
            CREATE TABLE {0}.{1}
            AS (

            SELECT DISTINCT
                b.anc_id
                ,c.nbh_cluster
                ,c.nbh_cluster_names
                ,d.smd_id
                ,e.ward_name
                ,f.census_tract
                ,g.comp_plan_area
                ,a.*
            FROM {2}.{3} a
            LEFT JOIN anc_boundaries b on a.{5} = b.{5}
            LEFT JOIN nbh_boundaries c on a.{5} = c.{5}
            LEFT JOIN smd_boundaries d on a.{5} = d.{5}
            LEFT JOIN ward_boundaries e on a.{5} = e.{5}
            LEFT JOIN census_tract_boundaries f on a.{5} = f.{5}
            LEFT JOIN comp_plan_boundaries g on a.{5} = g.{5}
            );

            CREATE INDEX {4} ON {0}.{1} USING GIST (geography);
        """.format(target_schema, target_table, from_schema, from_table, target_schema+'_'+target_table+'_index',partition_by_field, overlap_variable,join_type)
    
        print("executing non-point location query")
        engine.execute(non_point_location_query)

    # if desired, pass target schema and table to the next function
    return(target_schema,target_table)


def add_school_info(engine, from_schema:str, from_table:str, target_schema:str, target_table:str):

    # empty variable to store list of table columns
    columns_string =''

    # get column names of source table
    get_columns_query = """
    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{0}' AND TABLE_NAME = '{1}'
    """.format(from_schema, from_table)

    # put column names of source table in list
    columns = [r for (r,) in engine.execute(get_columns_query).fetchall()]
    columns_string+='a.'+columns[0]
    for column in columns[1:]:
        columns_string+=' ,a.'+column

    add_school_query="""
        DROP TABLE IF EXISTS {0}.{1};
        CREATE TABLE {0}.{1}
        AS (

        SELECT 
            ARRAY_AGG(distinct b.school_name) as near_schools
        , MAX(b.ES) as ES
        , MAX(b.MS) as MS
        , MAX(b.HS) as HS
        , MAX(b.public_school) as public_school
        , MAX(b.charter_school) as charter_school
        , a.* 
        FROM {2}.{3} a
        LEFT JOIN analysis_data.all_schools b on ST_DWithin(b.geography,a.geography,200)
        GROUP BY {5}
        ) ;

        CREATE INDEX {4} ON {0}.{1} USING GIST (geography);
    """.format(target_schema, target_table, from_schema, from_table, target_schema+'_'+target_table+'_index', columns_string)
    
    engine.execute(add_school_query)

    # if desired, pass target schema and table to the next function
    return(target_schema,target_table)

def add_roadway_info(engine, from_schema:str, from_table:str, target_schema:str, target_table:str, within_distance:float, partition_by_field:str):

    roadway_info_query="""
        DROP TABLE IF EXISTS {0}.{1};
        CREATE TABLE {0}.{1}
        AS (
            SELECT DISTINCT a.*
            ,ROW_NUMBER() over (partition by a.{4} order by ST_Distance(a.geography,b.geography)) as row_num
            ,ST_Distance(a.geography,b.geography) AS distance_to_nearest_block
            ,b.aadt
            ,b.totaltravellanes
            ,b.totalcrosssectionwidth
            ,b.totalparkinglanes
            ,b.doubleyellow_line
            ,b.summarydirection
            ,case 
                when b.sidewalk_ib_pavtype is not null and b.sidewalk_ob_pavtype is not null then 2
                when b.sidewalk_ib_pavtype is not null or b.sidewalk_ob_pavtype is not null then 1
                else 0 end as Num_Sides_W_Sidewalks
            ,coalesce(b.sidewalk_ib_width, b.sidewalk_ob_width) as sidewalk_width
            ,coalesce(b.speedlimits_ib, b.speedlimits_ob) as speed_limit
            ,b.dcfunctionalclass
            ,b.nhstype
            ,b.routename 
            ,b.objectid as block_objectid
            ,case dcfunctionalclass
                when '11.0' then 'Interstate'
                when '12.0' then 'Other Freeway and Expressway'
                when '14.0' then 'Principal Arterial'
                when '16.0' then 'Minor Arterial'
                when '17.0' then 'Collector'
                when '19.0' then 'Local'
                end as dcfunctionalclass_desc
            FROM {2}.{3} a
            LEFT JOIN source_data.roadway_blocks b on ST_DWithin(b.geography, a.geography,{5})
        ) ;
        DELETE FROM {0}.{1} WHERE row_num >1;

        ALTER TABLE {0}.{1}  DROP COLUMN row_num;

    """.format(target_schema, target_table, from_schema, from_table, partition_by_field, str(within_distance))

    engine.execute(roadway_info_query)

    # if desired, pass target schema and table to the next function
    return(target_schema,target_table)

def add_intersection_info(engine, from_schema:str, from_table:str, target_schema:str, target_table:str, within_distance:float, partition_by_field:str):

    intersection_info_query="""
        DROP TABLE IF EXISTS {0}.{1};
        CREATE TABLE {0}.{1}
        AS (
            SELECT DISTINCT a.*
            ,ROW_NUMBER() over (partition by a.{4} order by ST_Distance(a.geography,b.geography)) as row_num
            ,ST_Distance(a.geography,b.geography) AS distance_to_nearest_intersection
            ,b.intersectionid
            ,b.intersection_type
            ,b.int_road_types
            ,b.int_road_block_ids
            ,b.street_names
            FROM {2}.{3} a
            LEFT JOIN analysis_data.intersection_points b on ST_DWithin(b.geography, a.geography,{5})
        ) ;
        DELETE FROM {0}.{1} WHERE row_num >1;

        ALTER TABLE {0}.{1}  DROP COLUMN row_num;

    """.format(target_schema, target_table, from_schema, from_table, partition_by_field, str(within_distance))

    engine.execute(intersection_info_query)

    # if desired, pass target schema and table to the next function
    return(target_schema,target_table)


def create_final_table(engine, from_schema:str, from_table:str, target_schema:str, target_table:str):

    final_query="""
    DROP TABLE IF EXISTS {0}.{1};

    CREATE TABLE {0}.{1} AS 
        SELECT * FROM {2}.{3};
    CREATE INDEX {0}_{1}_index ON {0}.{1} USING GIST (geography);
    GRANT ALL PRIVILEGES ON {0}.{1} TO PUBLIC;
    """.format(target_schema, target_table, from_schema, from_table)

    engine.execute(final_query)
    
    count_query = 'SELECT COUNT(*) FROM {}.{}'.format(target_schema, target_table)
    
    row_count = engine.execute(count_query).fetchone()[0]

    return row_count
