import sqlalchemy
from connect_to_rds import get_connection_strings, create_postgres_engine
from add_location_info import add_location_info,add_school_info,add_walkscore_info,add_roadway_info,add_intersection_info,create_final_table
import argparse

def generate_waze_avg_speed (engine):

    step1_query = """
    DROP TABLE IF EXISTS tmp.waze_users;
    CREATE TABLE tmp.waze_users
    AS (
    SELECT DISTINCT
        *
        ,(scrape_datetime at time zone 'America/New_York')::date as day
        ,date_part('hour', scrape_datetime at time zone 'America/New_York') as hour_of_day
        , date_part('dow', scrape_datetime at time zone 'America/New_York') as day_of_week
        ,concat(user_id, source_file) as unique_row_id
    FROM source_data.waze_users_stream 
    );

    CREATE INDEX IF NOT EXISTS tmp_waze_users_geom_idx ON tmp.waze_users USING GIST (geography);
   
    """

    group_by_query="""
    DROP TABLE IF EXISTS tmp.avg_waze_speed_by_roadway_block;
    CREATE TABLE tmp.avg_waze_speed_by_roadway_block 
    AS (SELECT
        roadway_blockkey
        ,c.road_geography as geography
        ,day
        ,hour_of_day
        ,day_of_week
        ,aadt
        ,speed_limit
        ,dcfunctionalclass
        ,dcfunctionalclass_desc
        ,avg(distance_to_nearest_block) as avg_distance_from_block
        ,avg(case when user_speed>60.00 then NULL else user_speed end) as Avg_Speed
        ,avg(case when user_speed>60.00 then NULL else user_speed*2.237 end) as Avg_Speed_MPH
        ,count(distinct unique_row_id) as Num_Users
        ,count(distinct case when user_speed = 0 then unique_row_id else NULL end) as Num_Users_Stopped
        ,avg(case when user_speed>60.00 or user_speed = 0 then NULL else user_speed end) as Avg_Speed_Moving_Users
        ,avg(case when user_speed>60.00 or user_speed = 0 then NULL else user_speed*2.237 end) as Avg_Speed_MPH_Moving_Users
    FROM (select distinct a.*, b.geography as road_geography from tmp.waze_users_roadway_blocks a inner join analysis_data.roadway_blocks b on a.roadway_blockkey = b.blockkey) as c
    GROUP BY roadway_blockkey
        ,day
        ,hour_of_day
        ,day_of_week
        ,c.road_geography
        ,aadt
        ,speed_limit
        ,dcfunctionalclass
        ,dcfunctionalclass_desc
    ) 
    """

    percentile_query="""
    DROP TABLE IF EXISTS tmp.percentiles_waze_speed_by_roadway_block;
    CREATE TABLE tmp.percentiles_waze_speed_by_roadway_block 
    AS (SELECT
        roadway_blockkey
        ,c.road_geography as geography
        ,min(day) as Min_Date
        ,max(day) as Max_Date
        ,count(distinct case when user_speed - speed_limit between 10 and 20 then unique_row_id else NULL end) as Num_Users_10_20_over_limit
        ,count(distinct case when user_speed - speed_limit >20 then unique_row_id else NULL end) as Num_Users_20_plus_over_limit
        ,aadt
        ,speed_limit
        ,dcfunctionalclass
        ,dcfunctionalclass_desc
        ,avg(distance_to_nearest_block) as avg_distance_from_block
        ,avg(case when user_speed>60.00 then NULL else user_speed*2.237 end) as Avg_Speed_MPH
        ,count(distinct unique_row_id) as Num_Users
        ,count(distinct case when user_speed = 0 then unique_row_id else NULL end) as Num_Users_Stopped
        ,avg(case when user_speed>60.00 or user_speed = 0 then NULL else user_speed*2.237 end) as Avg_Speed_MPH_Moving_Users
        ,percentile_cont(0.25) within group (order by user_speed*2.237 asc) as percentile_25
  		,percentile_cont(0.50) within group (order by user_speed*2.237 asc) as percentile_50
  		,percentile_cont(0.75) within group (order by user_speed*2.237 asc) as percentile_75
  		,percentile_cont(0.95) within group (order by user_speed*2.237 asc) as percentile_95
		,percentile_cont(0.99) within group (order by user_speed*2.237 asc) as percentile_99
        ,percentile_cont(0.25) within group (order by case when user_speed = 0 then null else user_speed*2.237 end  asc) as percentile_25_moving_users
  		,percentile_cont(0.50) within group (order by case when user_speed = 0 then null else user_speed*2.237 end  asc) as percentile_50_moving_users
  		,percentile_cont(0.75) within group (order by case when user_speed = 0 then null else user_speed*2.237 end  asc) as percentile_75_moving_users
  		,percentile_cont(0.95) within group (order by case when user_speed = 0 then null else user_speed*2.237 end asc) as percentile_95_moving_users
		,percentile_cont(0.99) within group (order by case when user_speed = 0 then null else user_speed*2.237 end  asc) as percentile_99_moving_users
    FROM (select distinct a.*, b.geography as road_geography from tmp.waze_users_roadway_blocks a inner join analysis_data.roadway_blocks b on a.roadway_blockkey = b.blockkey) as c
    GROUP BY roadway_blockkey
        
        ,c.road_geography
        ,aadt
        ,speed_limit
        ,dcfunctionalclass
        ,dcfunctionalclass_desc 

    ) 
    """

    # First execute the table-specific queries
    engine.execute(step1_query)
    print("step1 query complete")
    add_roadway_info(engine=engine, target_schema='tmp', target_table='waze_users_roadway_blocks', from_schema='tmp', from_table='waze_users', partition_by_field='unique_row_id', within_distance= 20)
    print("roadway info query complete")
    engine.execute(group_by_query)
    print("group by query complete")
    engine.execute(percentile_query)
    print("percentile_queryquery complete")
    row_count = create_final_table(engine=engine, target_schema = 'analysis_data', target_table='avg_waze_speed_by_roadway_block', from_schema='tmp', from_table='avg_waze_speed_by_roadway_block')
    print("final query complete with row count ",row_count)
    row_count = create_final_table(engine=engine, target_schema = 'analysis_data', target_table='percentiles_waze_speed_by_roadway_block', from_schema='tmp', from_table='percentiles_waze_speed_by_roadway_block')
    print("final percentiles query complete with row count ",row_count)

    # # Then execute the same location-info queries (roadway, schools, neighborhoods) that apply to all analysis tables and create the final table
    # next_tables = add_location_info(engine=engine, target_schema='tmp', target_table='all311_nbh_ward', from_schema='tmp', from_table='all311_filtered', partition_by_field='objectid')
    # print("neighborhood-ward query complete")
    # next_tables = add_school_info(engine=engine, target_schema='tmp', target_table='all311_schools', from_schema=next_tables[0], from_table=next_tables[1])
    # print("schools query complete")
    # next_tables = add_walkscore_info(engine=engine, target_schema='tmp', target_table='all311_walkscore', from_schema=next_tables[0], from_table=next_tables[1])
    # print("walkscore query complete")
    # next_tables = add_roadway_info(engine=engine, target_schema='tmp', target_table='all311_roadway_blocks', from_schema=next_tables[0], from_table=next_tables[1], partition_by_field='objectid', within_distance= 40)
    # print("roadway info query complete")
    # next_tables = add_intersection_info(engine=engine, target_schema='tmp', target_table='all311_intersection_info', from_schema=next_tables[0], from_table=next_tables[1], partition_by_field='objectid', within_distance= 30)
    # print("intersection info query complete")
    # row_count = create_final_table(engine=engine, target_schema = 'analysis_data', target_table='all311', from_schema=next_tables[0], from_table=next_tables[1])
    # print("final query complete with row count ",row_count)


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
    generate_waze_avg_speed(engine)