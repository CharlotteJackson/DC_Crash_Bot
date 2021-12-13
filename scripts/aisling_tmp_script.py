from connect_to_rds import get_connection_strings, create_postgres_engine
from add_location_info import add_location_info,add_school_info,add_roadway_info, add_intersection_info,create_final_table,geocode_text
import argparse

def mv_data_prep(engine):
    #Move all source data records to a temp table and cast variables
    step_1_query="""
    DROP TABLE IF EXISTS tmp.moving_violations;
    CREATE TABLE tmp.moving_violations AS (
        SELECT DISTINCT
        objectid
        ,location
        ,xcoord::numeric
        ,ycoord::numeric

        ,issue_date::TIMESTAMP::DATE as issue_date
        ,issue_time
        ,issuing_agency_code
        ,issuing_agency_name
        ,issuing_agency_short
        ,violation_code
        ,violation_process_desc
        ,plate_state
        ,accident_indicator
        ,disposition_code
        ,disposition_type
        ,fine_amount::numeric
        ,total_paid::numeric
        ,penalty_1::numeric
        ,penalty_2::numeric
        ,penalty_3::numeric
        ,penalty_4::numeric
        ,penalty_5::numeric
        ,rp_mult_owner_no
        ,body_style
        ,latitude::numeric
        ,longitude::numeric
        ,mar_id
        ,drv_lic_state
        ,dob_year::numeric
        ,veh_year::numeric
        ,veh_make
        ,geometry as geography_raw
        ,case when geometry is not null then ST_Force2D(ST_GeomFromText(geometry, 4326))::geography 
            when geometry is null and latitude is not null then ST_SetSRID(ST_MakePoint(longitude::numeric, latitude::numeric),4326)::geography 
            else null end AS geography
        FROM source_data.moving_violations
    );
    """

    engine.execute(step_1_query)
    print("temp table created")

    # Separate entries into those with geo locations and those without
    step_2_query="""
    DROP TABLE IF EXISTS tmp.moving_violations_need_geo;
    CREATE TABLE tmp.moving_violations_need_geo as 
    SELECT * FROM tmp.moving_violations
    WHERE geography IS NULL;
    DROP TABLE IF EXISTS tmp.moving_violations_has_geo;
    CREATE TABLE tmp.moving_violations_has_geo as 
    SELECT * FROM tmp.moving_violations
    WHERE geography IS NOT NULL;
    CREATE INDEX IF NOT EXISTS mv_location_index ON tmp.moving_violations_need_geo (location);
    """

    engine.execute(step_2_query)
    print("temp table created")

    #Geocode the locations where needed
    records = [loc for (loc,) in engine.execute("select distinct location from tmp.moving_violations_need_geo where geography is null limit 500").fetchall()]
    print(len(records)," records passed to geocode function")
    geocode_text(engine=engine, records_to_geocode = records, administrative_area='District of Columbia', text_type = 'Moving Violations location')

    # update lat and long values from new data
    step_3_query = """
    UPDATE tmp.moving_violations_need_geo
    SET geography = source_data.geocoded_text.point_geography
    FROM source_data.geocoded_text 
    WHERE source_data.geocoded_text.text = location
    ;
    INSERT INTO tmp.moving_violations_has_geo
    SELECT * FROM tmp.moving_violations_need_geo;
    CREATE INDEX IF NOT EXISTS mv_geom_idx ON tmp.moving_violations_has_geo USING GIST (geography);
    """

    engine.execute(step_3_query)
    print("geo values updated")

    # ########################################################
    # NEW: link in roadway block with MAR ID where possible.
    # where not possible, join to closest NON-LOCAL road
    #########################################################

    # link in roadway seg id on MAR ID where possible
    mar_link_query = """
    DROP TABLE IF EXISTS tmp.mv_mar_link;
    CREATE TABLE tmp.mv_mar_link AS
    SELECT DISTINCT a.*
        ,b.geography as block_geography
        ,b.block_name
        ,b.aadt
            ,b.totaltravellanes
            ,b.totalcrosssectionwidth
            ,coalesce(b.speedlimits_ib, b.speedlimits_ob) as speed_limit
            ,b.dcfunctionalclass
            ,b.nhstype
            ,b.routename 
            ,b.objectid as block_objectid
            ,b.blockkey as roadway_blockkey
            ,case dcfunctionalclass
                when '11.0' then 'Interstate'
                when '12.0' then 'Other Freeway and Expressway'
                when '14.0' then 'Principal Arterial'
                when '16.0' then 'Minor Arterial'
                when '17.0' then 'Collector'
                when '19.0' then 'Local'
                end as dcfunctionalclass_desc
            ,'MAR' as link_type
    FROM tmp.moving_violations_has_geo a
    LEFT JOIN source_data.roadway_blocks b on a.mar_id::numeric = b.mar_id
    """

    engine.execute(mar_link_query)
    print("mar linked in")

    # where NOT possible: link to nearest NON LOCAL road
    geo_match_query = """
    DROP TABLE IF EXISTS tmp.mv_geo_link;
    CREATE TABLE tmp.mv_geo_link AS
    SELECT DISTINCT a.*
        ,ROW_NUMBER() over (partition by a.objectid order by ST_Distance(a.geography,b.geography)) as row_num
        ,b.geography as block_geography
        ,b.block_name
        ,b.aadt
            ,b.totaltravellanes
            ,b.totalcrosssectionwidth
            ,coalesce(b.speedlimits_ib, b.speedlimits_ob) as speed_limit
            ,b.dcfunctionalclass
            ,b.nhstype
            ,b.routename 
            ,b.objectid as block_objectid
            ,b.blockkey as roadway_blockkey
            ,case b.dcfunctionalclass
                when '11.0' then 'Interstate'
                when '12.0' then 'Other Freeway and Expressway'
                when '14.0' then 'Principal Arterial'
                when '16.0' then 'Minor Arterial'
                when '17.0' then 'Collector'
                when '19.0' then 'Local'
                end as dcfunctionalclass_desc
            ,'GEO' as link_type
    FROM tmp.moving_violations_has_geo a
    LEFT JOIN (select * from source_data.roadway_blocks where dcfunctionalclass <> '19.0') b on ST_DWithin(b.geography, a.geography,50)
    INNER JOIN tmp.mv_mar_link c on a.objectid = c.objectid AND c.roadway_blockkey IS NULL
    ;

     DELETE FROM tmp.mv_geo_link WHERE row_num >1;

    ALTER TABLE tmp.mv_geo_link DROP COLUMN row_num;
    """

    engine.execute(geo_match_query)
    print("nearest non local road linked in for records without a mar match")

    # unite the datasets
    combine_tables_query = """
    DROP TABLE IF EXISTS tmp.moving_violations_prepped_data;
    CREATE TABLE tmp.moving_violations_prepped_data AS
    SELECT *
    FROM tmp.mv_mar_link
    WHERE roadway_blockkey IS NOT NULL
    UNION ALL
    SELECT *
    FROM tmp.mv_geo_link
    ;
    """

    engine.execute(combine_tables_query)
    print("data prep finished")

    # ########################################################
    # END NEW
    #########################################################

    return('tmp','moving_violations_prepped_data')

def add_automated_camera_info(engine, from_schema: str, from_table: str, target_schema: str, target_table: str):


    # empty variable to store list of table columns
    columns_string = ''

    # get column names of source table
    get_columns_query = """
    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{0}' AND TABLE_NAME = '{1}'
    """.format(from_schema, from_table)

    # put column names of source table in list
    columns = [r for (r,) in engine.execute(get_columns_query).fetchall()]
    columns_string += 'a.' + columns[0]
    for column in columns[1:]:
        columns_string += ' ,a.' + column

    # A ticket is suspect of being automatic if:
    # violation_process_desc is one of the top 10 violation types across all data
    # The ticket was issued by the Special Operations & Traffic Divison (agency code =25)
    # The geography value (i.e. location) has more than 100 tickets at that spot

    # Make a temporary table, moving_violations_auto_data, which determines suspected automated camera tickets and marks their type
    automated_camera_query = """
        DROP TABLE IF EXISTS {0}.{1};
        CREATE TABLE {0}.{1} AS 
        (WITH geog AS 
            (SELECT geography, COUNT(*) as count_
            FROM {2}.{3} 
            WHERE issuing_agency_code='25' 
            GROUP BY geography
            HAVING COUNT(*)>100
            ORDER BY count_ DESC)
        SELECT *, 1 AS suspected_automatic, 
                CASE WHEN violation_process_desc LIKE '%%SPEED%%' THEN 'SPEED SAFETY CAMERA'
                WHEN violation_process_desc LIKE '%%MPH%%' THEN 'SPEED SAFETY CAMERA'
                WHEN violation_process_desc LIKE '%%RED%%' THEN 'RED LIGHT SAFETY CAMERA'
                WHEN violation_process_desc LIKE '%%FULL%%' THEN 'STOP SIGN SAFETY CAMERA'
                ELSE 'NONE'
                END AS camera_type
	    FROM {2}.{3} 
	    WHERE issuing_agency_code='25' 
	    AND geography IN (SELECT geography FROM geog)
	    AND violation_code IN ('T119', 'T120', 'T113', 'T121', 'T128', 'T334', 'T122', 'T823', 'T822', 'T202')
	    UNION
        SELECT *, 0 AS suspected_automatic, 'NONE' as camera_type
        FROM {2}.{3} 
	    WHERE issuing_agency_code!='25' 
	    OR geography NOT IN (SELECT geography FROM geog)
	    OR violation_code NOT IN ('T119', 'T120', 'T113', 'T121', 'T128', 'T334', 'T122', 'T823', 'T822', 'T202')
        );
        CREATE INDEX {4} ON {0}.{1} USING GIST (geography);
        """.format(target_schema, target_table, from_schema, from_table, target_schema + '_' + target_table + '_index',
                   columns_string)

    engine.execute(automated_camera_query)

    # if desired, pass target schema and table to the next function
    return (target_schema, target_table)

def generate_moving_violations_table (engine, **kwargs):
    # next_tables=mv_data_prep(engine=engine)
    # print('added geos and linked in roadway info')
    next_tables=add_automated_camera_info(engine=engine, target_schema='tmp', target_table='moving_violations_automated_cameras', from_schema='tmp', from_table='moving_violations_prepped_data')
    print("automated camera query complete")
    row_count = create_final_table(engine=engine, target_schema='analysis_data', target_table='moving_violations', from_schema=next_tables[0], from_table=next_tables[1])
    print("final query complete with row count ",row_count)

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
        env = 'DEV'
    env = env.upper()
    engine = create_postgres_engine(destination="AWS_PostGIS", env=env)
    generate_moving_violations_table(engine=engine)