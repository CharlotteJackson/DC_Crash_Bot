import sqlalchemy
from connect_to_rds import get_connection_strings, create_postgres_engine
from add_location_info import geocode_text
import argparse

def generate_dc_fss (engine):

    step1_query = """
    DROP TABLE IF EXISTS source_data.dc_fss;
    CREATE TABLE source_data.dc_fss AS 
    (   SELECT uuid_generate_v1() AS Record_ID, *
        FROM stg."dc_fss.csv"
    );

    GRANT ALL PRIVILEGES ON source_data.dc_fss TO PUBLIC;
    """

    engine.execute(step1_query)

    roll_up_query = """
    DROP TABLE IF EXISTS tmp.dc_fss_rollup;
    CREATE TABLE tmp.dc_fss_rollup AS (
    select 
        uuid_generate_v1() as Record_ID
        ,load_datetime
        ,source_file
        ,case when u_crash_date = '2007' then '1-1-2007'::date else cast(u_crash_date as date) end as crash_date
        ,u_exact_location
        ,sum(case when u_mode ilike '%%pedestrian%%' then 1 else 0 end) as total_pedestrian_fatalities
        ,sum(case when u_mode ilike '%%bicyclist%%' then 1 else 0 end) as total_bicyclist_fatalities
        ,sum(case when u_mode not ilike '%%pedestrian%%' and u_mode not ilike '%%bicyclist%%' then 1 else 0 end) as total_vehicle_fatalities
        ,max(case when u_mode ilike '%%scooter%%' then 1 else 0 end) as Scooter_Flag
        ,max(case when u_mode ilike '%%motorcycl%%' then 1 else 0 end) as Motorcycle_Flag
        ,max(case when u_primary_ward not in ('1','2','3','4','5','6','7','8','6/5','') then 1 else 0 end) as Not_In_DC
        ,max(u_mpd_press_release) as u_mpd_press_release
    from stg."dc_fss.csv"
    group by 
    load_datetime
        ,source_file
        ,case when u_crash_date = '2007' then '1-1-2007'::date else cast(u_crash_date as date) end
        ,u_exact_location
        );
    """

    engine.execute(roll_up_query)

    # geocode records
    records = [r for (r,) in engine.execute("select distinct u_exact_location from tmp.dc_fss_rollup").fetchall()]
    print(len(records)," records passed to geocode function")
    geocode_text(engine=engine, records_to_geocode = records, administrative_area='District of Columbia', text_type = 'DC FSS address')

    join_query = """
    DROP TABLE IF EXISTS source_data.dc_fss_rollup;
    CREATE TABLE source_data.dc_fss_rollup AS (
    SELECT DISTINCT a.* 
    ,case when c.geography is not null or u_mpd_press_release ilike '%%Park Police%%' then 1 else 0 end as National_Park
    ,b.point_type
    ,b.point_geography
    ,b.polygon_geography
    from tmp.dc_fss_rollup a
    LEFT JOIN source_data.geocoded_text b on a.u_exact_location = b.text
    LEFT JOIN source_data.national_parks c on ST_Intersects(b.point_geography, c.geography)
        );

    GRANT ALL PRIVILEGES ON source_data.dc_fss_rollup TO PUBLIC;
    """

    engine.execute(join_query)


# command line arguments
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
        env='DEV'
    env=env.upper()

    # set up RDS and S3 connections, engines, cursors
    engine = create_postgres_engine(destination="AWS_PostGIS", env=env)
    generate_dc_fss(engine)