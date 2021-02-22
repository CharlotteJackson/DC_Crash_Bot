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
    query = """
    drop table if exists viz.{0}_boundaries;
    create table viz.{0}_boundaries as
    SELECT a.{1} as {0},
        (ST_Dumppoints(a.geography::geometry)).path[2] as POINT_ORDER
        ,ST_X((ST_Dumppoints(a.geography::geometry)).geom) as Latitude
        ,ST_Y((ST_Dumppoints(a.geography::geometry)).geom) as Longitude
    FROM {2} a;

    drop table if exists {0};
    create temp table {0} on commit preserve rows as (
    select 
		{1} as {0}
		,geography
		,st_area(geography::geography)/(1000.0^2) as area_in_km2
		,st_area(geography::geography) as area_in_m2
    from {2}
	) with data;

    drop table if exists {0}_2;
    create temp table {0}_2 on commit preserve rows as (
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
    from {0} a
        inner join (select {0}
                    , sum(total_pop) as total_pop_2019
                    , sum(total_households) as num_households
                    , sum(num_households_w_car) as num_households_w_car
                    ,sum(fagi_total_2015)/sum(total_pop*1.00) as fagi_per_capita_2015
                    from analysis_data.acs_2019_by_tract
                    group by {0}) b on b.{0}=a.{0}
        left join (select {0}
                    , sum(total_pop) as total_pop_2010
                    , sum(pop_white_non_hispanic)/sum(total_pop*1.00) as pct_white
                    , sum(pop_non_hispanic_black)/sum(total_pop*1.00) as pct_black
                    , sum(pop_hispanic)/sum(total_pop*1.00) as pct_hispanic
                    , sum(pop_non_hispanic_asian)/sum(total_pop*1.00) as pct_asian
                    from analysis_data.census_blocks
                    where total_pop > 0
                    group by {0}) c on c.{0}=a.{0}
        ) with data;

    drop table if exists {0}_3;
    create temp table {0}_3 on commit preserve rows as (
    select a.*
        ,cast(b.daily_riders as decimal(10,2)) as daily_riders 
    from {0}_2 a
    left join (select 
                    {0}
                    ,sum(avg_2015_2018) as daily_riders 
                from analysis_data.metro_stations_ridership 
                group by {0} ) b on b.{0}= a.{0} 
    ) with data;


    drop table if exists {0}_4;
    create temp table {0}_4 on commit preserve rows as (
    select a.*
        ,b.*
        from {0}_3 a
        inner join (select 
					{0} as {0}_extra1
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
				group by {0}
			   ) b on b.{0}_extra1= a.{0}
            ) with data;

            drop table if exists {0}_5;
            create temp table {0}_5 on commit preserve rows as (
            select a.*
                ,b.num_311_requests
                ,b.num_311_requests_granted
            from {0}_4 a
            left join (select 
                            {0}
                            ,count(*) as num_311_requests
                            ,sum(case when cwo_objectid is not null then 1 else 0 end) as num_311_requests_granted
                        from analysis_data.all311 
                        group by {0}) b on b.{0}= a.{0} 
            ) with data;

        
        drop table if exists {0}_6;
        create temp table {0}_6 on commit preserve rows as (
        select a.*
            ,b.*
        from {0}_5 a
        left join (select 
                        {0} as {0}_extra2
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
                    group by {0} ) b on b.{0}_extra2= a.{0} 
        ) with data;

        drop table if exists viz.{0}_statistics;
        create table viz.{0}_statistics as 
        select * from {0}_6;

        alter table viz.{0}_statistics drop column {0}_extra2;
        alter table viz.{0}_statistics drop column {0}_extra1;

    """.format(geo_level, geography_levels[geo_level]['orig_field_name'], geography_levels[geo_level]['geo_boundaries_source_table'])

    engine.execute(query)
    print("tables created for geo ", geo_level)