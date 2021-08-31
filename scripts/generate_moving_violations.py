from connect_to_rds import get_connection_strings, create_postgres_engine
from add_location_info import add_location_info,add_school_info,add_roadway_info, add_intersection_info,create_final_table,geocode_text
import argparse

def generate_moving_violations_table (engine, **kwargs):

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
    records = [loc for (loc,) in engine.execute("select distinct location from tmp.moving_violations_need_geo where geography is null limit 100").fetchall()]
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

    #Original Code
    # Then execute the same location-info queries (roadway, schools, neighborhoods) that apply to all analysis tables and create the final table
    next_tables = add_location_info(engine=engine, target_schema='tmp', target_table='moving_violations_nbh_ward', from_schema='tmp', from_table='moving_violations_has_geo', partition_by_field='objectid')
    print("neighborhood-ward query complete")
    next_tables = add_school_info(engine=engine, target_schema='tmp', target_table='moving_violations_schools', from_schema=next_tables[0], from_table=next_tables[1])
    print("schools query complete")
    next_tables = add_roadway_info(engine=engine, target_schema='tmp', target_table='moving_violations_roadway_info', from_schema=next_tables[0], from_table=next_tables[1], partition_by_field='objectid', within_distance= 70)
    print("roadway info query complete")
    next_tables = add_intersection_info(engine=engine, target_schema='tmp', target_table='moving_violations_intersection_info', from_schema=next_tables[0], from_table=next_tables[1], partition_by_field='objectid', within_distance= 20)
    print("intersection info query complete")
    row_count = create_final_table(engine=engine, target_schema = 'analysis_data', target_table='moving_violations', from_schema=next_tables[0], from_table=next_tables[1])
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