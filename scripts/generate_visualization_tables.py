import sqlalchemy
from connect_to_rds import get_connection_strings, create_postgres_engine
from add_location_info import add_location_info,add_school_info,create_final_table


dbname='postgres'
env="DEV"
engine = create_postgres_engine(destination="AWS_PostGIS", target_db=dbname, env=env)
db_credentials = get_connection_strings("AWS_PostGIS")

geography_levels = {
    'comp_plan_area': {
        'geo_boundaries_source_table': 'source_data.comp_plan_areas'
        ,'orig_field_name': 'name'}
    ,'census_tract': {
        'geo_boundaries_source_table': 'source_data.census_tracts'
        ,'orig_field_name': 'tract'}
    ,'nbh_cluster_names': {
        'geo_boundaries_source_table': 'source_data.neighborhood_clusters'
        ,'orig_field_name': 'nbh_names'}
    ,'ward_name': {
        'geo_boundaries_source_table': 'source_data.ward_boundaries'
        ,'orig_field_name': 'name'}
    ,'anc_id': {
        'geo_boundaries_source_table': 'source_data.anc_boundaries'
        ,'orig_field_name': 'anc_id'}
    ,'smd_id': {
        'geo_boundaries_source_table': 'source_data.smd_boundaries'
        ,'orig_field_name': 'smd_id'}

}

for geo_level in geography_levels.keys():
    query = f"""
    drop table if exists viz.{geo_level}_boundaries;
    create table viz.{geo_level}_boundaries as
    SELECT a.{geography_levels[geo_level]['orig_field_name']} as {geo_level},
        (ST_Dumppoints(a.geography::geometry)).path[2] as POINT_ORDER
        ,ST_X((ST_Dumppoints(a.geography::geometry)).geom) as Longitude
        ,ST_Y((ST_Dumppoints(a.geography::geometry)).geom) as Latitude
    FROM {geography_levels[geo_level]['geo_boundaries_source_table'])} a;

    drop table if exists {geo_level};
    create temp table {geo_level} on commit preserve rows as (
    select 
		{geography_levels[geo_level]['orig_field_name']} as {geo_level}
		,geography
		,st_area(geography::geography)/(1000.0^2) as area_in_km2
		,st_area(geography::geography) as area_in_m2
    from {geography_levels[geo_level]['geo_boundaries_source_table'])}
	) with data;

    drop table if exists {geo_level}_2;
    create temp table {geo_level}_2 on commit preserve rows as (
    select 
            a.*
            ,case when c.total_pop_2010 is null then 0 else c.total_pop_2010 end as total_pop_2010
            ,case when c.pct_white is null then 0 else c.pct_white end as pct_white_2010
            ,case when c.pct_black is null then 0 else c.pct_black end as pct_black_2010
            ,case when c.pct_hispanic is null then 0 else c.pct_hispanic end as pct_hispanic_2010
            ,case when c.pct_asian is null then 0 else c.pct_asian end as pct_asian_2010
            ,b.total_pop_2019
            ,b.num_households
            ,b.num_households_w_car
            ,b.num_households_w_car/(b.num_households*1.00) as pct_households_w_car
            ,b.fagi_per_capita_2015
    from {geo_level} a
        inner join (select {geo_level}
                    , sum(total_pop) as total_pop_2019
                    , sum(total_households) as num_households
                    , sum(num_households_w_car) as num_households_w_car
                    ,sum(fagi_total_2015)/sum(total_pop*1.00) as fagi_per_capita_2015
                    from analysis_data.acs_2019_by_tract
                    group by {geo_level}) b on b.{geo_level}=a.{geo_level}
        left join (select {geo_level}
                    , sum(total_pop) as total_pop_2010
                    , sum(pop_white_non_hispanic)/sum(total_pop*1.00) as pct_white
                    , sum(pop_non_hispanic_black)/sum(total_pop*1.00) as pct_black
                    , sum(pop_hispanic)/sum(total_pop*1.00) as pct_hispanic
                    , sum(pop_non_hispanic_asian)/sum(total_pop*1.00) as pct_asian
                    from analysis_data.census_blocks
                    where total_pop > 0
                    group by {geo_level}) c on c.{geo_level}=a.{geo_level}
        ) with data;

    drop table if exists {geo_level}_3;
    create temp table {geo_level}_3 on commit preserve rows as (
    select a.*
        ,cast(b.daily_riders as decimal(10,2)) as daily_riders 
    from {geo_level}_2 a
    left join (select 
                    {geo_level}
                    ,sum(avg_2015_2018) as daily_riders 
                from analysis_data.metro_stations_ridership 
                group by {geo_level} ) b on b.{geo_level}= a.{geo_level} 
    ) with data;


    drop table if exists {geo_level}_4;
    create temp table {geo_level}_4 on commit preserve rows as (
    select a.*
        ,b.*
        from {geo_level}_3 a
        inner join (select 
					{geo_level} as {geo_level}_extra1
					--Pedestrians stats
					,sum(total_pedestrians) as num_peds_struck
					,sum(pedestrian_fatalities) as num_peds_killed
					,sum(peds_under_12) as peds_under_12
					,sum(peds_over_65) as peds_over_65
					,sum(case when total_pedestrians>0 then oos_vehicles else 0 end) as peds_struck_by_oos_drivers
					,sum(case when total_pedestrians>0 then driver_tickets else 0 end) as ped_strike_driver_ticketed
					,sum(case when total_pedestrians>0 then ped_tickets else 0 end) as ped_strike_ped_ticketed
					,avg(case when total_pedestrians>0 then aadt else null end) as avg_aadt_ped_accidents
					,avg(case when total_pedestrians>0 then totalcrosssectionwidth else null end) as avg_road_width_ped_accidents
                    ,avg(case when total_pedestrians>0 and dcfunctionalclass_desc = 'Local' then totalcrosssectionwidth else null end) as avg_local_road_width_ped_accidents
                    ,avg(case when total_pedestrians>0 and dcfunctionalclass_desc in ('Principal Arterial','Minor Arterial') then totalcrosssectionwidth else null end) as avg_arterial_road_width_ped_accidents
					,avg(case when total_pedestrians>0 then speed_limit else null end) as avg_speed_limit_ped_accidents
					,sum(case when near_schools[1] is not null then total_pedestrians else 0 end) as peds_struck_near_schools 
					,sum(case when dcfunctionalclass_desc = 'Local' then total_pedestrians else 0 end) as peds_struck_on_local_roads
                    ,sum(case when dcfunctionalclass_desc = 'Local' and intersectionid is null then total_pedestrians else 0 end) as peds_struck_on_local_roads_outside_intersections
                    ,sum(case when dcfunctionalclass_desc = 'Local' and ('Minor Arterial' = ANY(int_road_types) or 'Principal Arterial' = ANY(int_road_types)) then total_pedestrians else 0 end) as peds_struck_on_local_roads_at_arterial_intersections
                    ,sum(case when dcfunctionalclass_desc = 'Local' and 'Local' = ANY(int_road_types) then total_pedestrians else 0 end) as peds_struck_on_local_roads_at_local_intersections
                    ,sum(case when dcfunctionalclass_desc = 'Collector' then total_pedestrians else 0 end) as peds_struck_on_collectors
                    ,sum(case when dcfunctionalclass_desc = 'Collector' and intersectionid is null then total_pedestrians else 0 end) as peds_struck_on_collectors_outside_intersections
                    ,sum(case when dcfunctionalclass_desc = 'Collector' and ('Minor Arterial' = ANY(int_road_types) or 'Principal Arterial' = ANY(int_road_types)) then total_pedestrians else 0 end) as peds_struck_on_collectors_at_arterial_intersections
					,sum(case when dcfunctionalclass_desc = 'Minor Arterial' then total_pedestrians else 0 end) as peds_struck_on_minor_arterials
					,sum(case when dcfunctionalclass_desc = 'Principal Arterial' then total_pedestrians else 0 end) as peds_struck_on_principal_arterials
					,sum(case when dcfunctionalclass_desc in ('Principal Arterial','Minor Arterial') then total_pedestrians else 0 end) as peds_struck_on_any_arterial
                    ,sum(case when dcfunctionalclass_desc in ('Principal Arterial','Minor Arterial') and intersectionid is null then total_pedestrians else 0 end) as peds_struck_on_any_arterial_outside_intersections
					--Biker stats
					,sum(total_bicyclists) as num_bicyclists_struck
					,sum(bicycle_fatalities) as num_bicyclists_killed
					,sum(bikers_under_18) as bikers_under_18
					,sum(bikers_over_65) as bikers_over_65
					,sum(case when total_bicyclists>0 then oos_vehicles else 0 end) as bikers_struck_by_oos_drivers
					,sum(case when total_bicyclists>0 then driver_tickets else 0 end) as bike_strike_driver_ticketed
					,sum(case when total_bicyclists>0 then ped_tickets else 0 end) as bike_strike_biker_ticketed
					,avg(case when total_bicyclists>0 then aadt else null end) as avg_aadt_bike_accidents
					,avg(case when total_bicyclists>0 then totalcrosssectionwidth else null end) as avg_road_width_bike_accidents
                    ,avg(case when total_bicyclists>0 and dcfunctionalclass_desc = 'Local' then totalcrosssectionwidth else null end) as avg_local_road_width_bike_accidents
                    ,avg(case when total_bicyclists>0 and dcfunctionalclass_desc in ('Principal Arterial','Minor Arterial') then totalcrosssectionwidth else null end) as avg_arterial_road_width_bike_accidents
					,avg(case when total_bicyclists>0 then speed_limit else null end) as avg_speed_limit_bike_accidents
					,sum(case when near_schools[1] is not null then total_bicyclists else 0 end) as bikers_struck_near_schools 
					,sum(case when dcfunctionalclass_desc = 'Local' then total_bicyclists else 0 end) as bikers_struck_on_local_roads
                    ,sum(case when dcfunctionalclass_desc = 'Local' and intersectionid is null then total_bicyclists else 0 end) as bikers_struck_on_local_roads_outside_intersections
                    ,sum(case when dcfunctionalclass_desc = 'Local' and ('Minor Arterial' = ANY(int_road_types) or 'Principal Arterial' = ANY(int_road_types)) then total_bicyclists else 0 end) as bikers_struck_on_local_roads_at_arterial_intersections
                    ,sum(case when dcfunctionalclass_desc = 'Local' and 'Local' = ANY(int_road_types) then total_bicyclists else 0 end) as bikers_struck_on_local_roads_at_local_intersections
                    ,sum(case when dcfunctionalclass_desc = 'Collector' then total_bicyclists else 0 end) as bikers_struck_on_collectors
                    ,sum(case when dcfunctionalclass_desc = 'Collector' and intersectionid is null then total_bicyclists else 0 end) as bikers_struck_on_collectors_outside_intersections
                    ,sum(case when dcfunctionalclass_desc = 'Collector' and ('Minor Arterial' = ANY(int_road_types) or 'Principal Arterial' = ANY(int_road_types)) then total_bicyclists else 0 end) as bikers_struck_on_collectors_at_arterial_intersections
					,sum(case when dcfunctionalclass_desc = 'Minor Arterial' then total_bicyclists else 0 end) as bikers_struck_on_minor_arterials
					,sum(case when dcfunctionalclass_desc = 'Principal Arterial' then total_bicyclists else 0 end) as bikers_struck_on_principal_arterials
					,sum(case when dcfunctionalclass_desc in ('Principal Arterial','Minor Arterial') then total_bicyclists else 0 end) as bikers_struck_on_any_arterial
                    ,sum(case when dcfunctionalclass_desc in ('Principal Arterial','Minor Arterial') and intersectionid is null then total_bicyclists else 0 end) as bikers_struck_on_any_arterial_outside_intersections
				from analysis_data.dc_crashes_w_details 
				where date_part('year', fromdate) between 2015 and 2020
				group by {geo_level}
			   ) b on b.{geo_level}_extra1= a.{geo_level}
            ) with data;

            drop table if exists {geo_level}_5;
            create temp table {geo_level}_5 on commit preserve rows as (
            select a.*
                ,b.num_311_requests
                ,b.num_311_requests_granted
            from {geo_level}_4 a
            left join (select 
                            {geo_level}
                            ,count(*) as num_311_requests
                            ,sum(case when cwo_objectid is not null then 1 else 0 end) as num_311_requests_granted
                        from analysis_data.all311 
                        group by {geo_level}) b on b.{geo_level}= a.{geo_level} 
            ) with data;

        
        drop table if exists {geo_level}_6;
        create temp table {geo_level}_6 on commit preserve rows as (
        select a.*
            ,b.*
        from {geo_level}_5 a
        left join (select 
                        {geo_level} as {geo_level}_extra2
                        ,sum(ST_Length(geography)/1000.00) as total_road_km
                        ,avg(aadt) as avg_aadt
                        ,avg(totalcrosssectionwidth) as avg_road_width
                        ,avg(case when dcfunctionalclass_desc in ('Local') then totalcrosssectionwidth else null end) as avg_local_road_width
                        ,avg(case when dcfunctionalclass_desc in ('Collector') then totalcrosssectionwidth else null end) as avg_collector_road_width
                        ,avg(case when dcfunctionalclass_desc in ('Minor Arterial') then totalcrosssectionwidth else null end) as avg_minor_arterial_road_width
                        ,avg(case when dcfunctionalclass_desc in ('Principal Arterial') then totalcrosssectionwidth else null end) as avg_major_arterial_road_width
                        ,avg(case when dcfunctionalclass_desc in ('Principal Arterial','Minor Arterial') then totalcrosssectionwidth else null end) as avg_arterial_road_width
                        ,avg(speed_limit) as avg_speed_limit
                        ,avg(case when dcfunctionalclass_desc in ('Local') then speed_limit else null end) as avg_local_road_speed_limit
                        ,avg(case when dcfunctionalclass_desc in ('Collector') then speed_limit else null end) as avg_collector_road_speed_limit
                        ,avg(case when dcfunctionalclass_desc in ('Minor Arterial') then speed_limit else null end) as avg_minor_arterial_road_speed_limit
                        ,avg(case when dcfunctionalclass_desc in ('Principal Arterial') then speed_limit else null end) as avg_major_arterial_road_speed_limit
                        ,avg(case when dcfunctionalclass_desc in ('Principal Arterial','Minor Arterial') then speed_limit else null end) as avg_arterial_road_speed_limit
                        ,sum(ST_Length(geography))/count(*) as avg_block_len_meters
                        ,sum(case when dcfunctionalclass_desc = 'Local' then ST_Length(geography)/1000.00 else 0 end) as local_blocks_km
                        ,sum(case when dcfunctionalclass_desc = 'Collector' then ST_Length(geography)/1000.00 else 0 end) as collector_blocks_km
                        ,sum(case when dcfunctionalclass_desc = 'Minor Arterial' then ST_Length(geography)/1000.00 else 0 end) as minor_arterial_blocks_km
                        ,sum(case when dcfunctionalclass_desc = 'Principal Arterial' then ST_Length(geography)/1000.00 else 0 end) as principal_arterial_blocks_km
                        ,sum(case when dcfunctionalclass_desc in ('Principal Arterial','Minor Arterial') then ST_Length(geography)/1000.00 else 0 end) as arterial_blocks_km
                    from analysis_data.roadway_blocks 
                    group by {geo_level} ) b on b.{geo_level}_extra2= a.{geo_level} 
        ) with data;

        drop table if exists viz.{geo_level}_statistics;
        create table viz.{geo_level}_statistics as 
        select * from {geo_level}_6;

        alter table viz.{geo_level}_statistics drop column {geo_level}_extra2;
        alter table viz.{geo_level}_statistics drop column {geo_level}_extra1;
    """

    engine.execute(query)
    print("tables created for geo ", geo_level)