/********************************************
Create all tables needed for visualizations
********************************************/

drop schema if exists viz cascade;
create schema viz;
grant all privileges on schema viz to public;

select * from source_data.pulsepoint order by scrape_datetime desc 

select * from analysis_data.roadway_blocks
where dcfunctionalclass_desc ilike '%local%' and (nbh_cluster_names ilike '%randle%' or nbh_cluster_names ilike '%naylor%')

select * from analysis_data.roadway_blocks
where dcfunctionalclass_desc ilike '%local%' and (nbh_cluster_names ilike '%palisades%' or nbh_cluster_names ilike '%spring valley%')

select 
	comp_plan_area
	,sum(ST_Length(geography)) as total_length 
	,sum(case when dcfunctionalclass_desc = 'Local' then ST_Length(geography) else 0 end) as local_roads
	,sum(case when dcfunctionalclass_desc ilike '%Arterial%' then ST_Length(geography) else 0 end) as arterials
	,sum(case when dcfunctionalclass_desc = 'Collector' then ST_Length(geography) else 0 end) as collectors
	,sum(case when dcfunctionalclass_desc = 'Other Freeway and Expressway' then ST_Length(geography) else 0 end) as freeways
	,sum(case when dcfunctionalclass_desc = 'Interstate' then ST_Length(geography) else 0 end) as Interstate
from analysis_data.roadway_blocks
group by comp_plan_area
order by comp_plan_area
------------------------------
--Comp plan area level
------------------------------
select 
/*create boundaries*/
drop table if exists viz.comp_plan_boundaries;
create table viz.comp_plan_boundaries as
SELECT a.name as COMP_PLAN_AREA,
      (ST_Dumppoints(a.geography::geometry)).path[2] as POINT_ORDER
	  ,ST_X((ST_Dumppoints(a.geography::geometry)).geom) as Latitude
	  ,ST_Y((ST_Dumppoints(a.geography::geometry)).geom) as Longitude
FROM source_data.comp_plan_areas a;

/*
Area in km2 
*/

drop table if exists comp_plan_area;
create temp table comp_plan_area on commit preserve rows as (
select 
		name as COMP_PLAN_AREA
		,geography
		,st_area(geography::geography)/(1000.0^2) as area_in_km2
		,st_area(geography::geography) as area_in_m2
from source_data.comp_plan_areas
	) with data;

/*
Census variables:
Total population
Households without a car
Federal AGI
*/

drop table if exists comp_plan_2;
create temp table comp_plan_2 on commit preserve rows as (
select 
		a.*
		,c.total_pop_2010
		,b.total_pop_2019
		,b.num_households
		,b.num_households_w_car
		,b.num_households_w_car/(b.num_households*1.00) as pct_households_w_car
		,b.fagi_per_capita_2015
from comp_plan_area a
	inner join (select COMP_PLAN_AREA
				, sum(total_pop) as total_pop_2019
				, sum(total_households) as num_households
				, sum(num_households_w_car) as num_households_w_car
				,sum(fagi_total_2015)/sum(total_pop*1.00) as fagi_per_capita_2015
				from analysis_data.acs_2019_by_tract
				group by COMP_PLAN_AREA) b on b.COMP_PLAN_AREA=a.COMP_PLAN_AREA
	inner join (select COMP_PLAN_AREA
				, sum(total_pop) as total_pop_2010
				from analysis_data.census_blocks
				group by COMP_PLAN_AREA) c on c.COMP_PLAN_AREA=a.COMP_PLAN_AREA
	) with data;

/*
Metro station ridership variables
*/
drop table if exists comp_plan_3;
create temp table comp_plan_3 on commit preserve rows as (
select a.*
	,cast(b.daily_riders as decimal(10,2)) as daily_riders 
from comp_plan_2 a
left join (select 
		   		comp_plan_area
		   		,sum(avg_2015_2018) as daily_riders 
		   	from analysis_data.metro_stations_ridership 
			group by comp_plan_area ) b on b.comp_plan_area= a.comp_plan_area 
) with data;
--select * from comp_plan_3
/*
Car crash variables
*/
drop table if exists comp_plan_4;
create temp table comp_plan_4 on commit preserve rows as (
select a.*
	,b.*
	from comp_plan_3 a
	inner join (select 
					comp_plan_area as compplanarea_extra1
					--Pedestrians stats
					,sum(total_pedestrians) as num_peds_struck
					,sum(pedestrian_fatalities) as num_peds_killed
					,sum(peds_under_12) as peds_under_12
					,sum(peds_over_80) as peds_over_80
					,sum(case when total_pedestrians>0 then oos_vehicles else 0 end) as peds_struck_by_oos_drivers
					,sum(case when total_pedestrians>0 then driver_tickets else 0 end) as ped_strike_driver_ticketed
					,sum(case when total_pedestrians>0 then ped_tickets else 0 end) as ped_strike_ped_ticketed
					,avg(case when total_pedestrians>0 then aadt else null end) as avg_aadt_ped_accidents
					,avg(case when total_pedestrians>0 then totalcrosssectionwidth else null end) as avg_road_width_ped_accidents
					,avg(case when total_pedestrians>0 then speed_limit else null end) as avg_speed_limit_ped_accidents
					,sum(case when near_schools[1] is not null then total_pedestrians else 0 end) as peds_struck_near_schools 
					,sum(case when dcfunctionalclass_desc = 'Local' then total_pedestrians else 0 end) as peds_struck_on_local_roads
					,sum(case when dcfunctionalclass_desc = 'Minor Arterial' then total_pedestrians else 0 end) as peds_struck_on_minor_arterials
					,sum(case when dcfunctionalclass_desc = 'Principal Arterial' then total_pedestrians else 0 end) as peds_struck_on_principal_arterials
					,sum(case when dcfunctionalclass_desc = 'Collector' then total_pedestrians else 0 end) as peds_struck_on_collectors
					--Biker stats
					,sum(total_bicyclists) as num_bicyclists_struck
					,sum(bicycle_fatalities) as num_bicyclists_killed
					,sum(bikers_under_18) as bikers_under_18
					,sum(bikers_over_70) as bikers_over_70
					,sum(case when total_bicyclists>0 then oos_vehicles else 0 end) as bikers_struck_by_oos_drivers
					,sum(case when total_bicyclists>0 then driver_tickets else 0 end) as bike_strike_driver_ticketed
					,sum(case when total_bicyclists>0 then ped_tickets else 0 end) as bike_strike_biker_ticketed
					,avg(case when total_bicyclists>0 then aadt else null end) as avg_aadt_bike_accidents
					,avg(case when total_bicyclists>0 then totalcrosssectionwidth else null end) as avg_road_width_bike_accidents
					,avg(case when total_bicyclists>0 then speed_limit else null end) as avg_speed_limit_bike_accidents
					,sum(case when near_schools[1] is not null then total_bicyclists else 0 end) as bikers_struck_near_schools 
					,sum(case when dcfunctionalclass_desc = 'Local' then total_bicyclists else 0 end) as bikers_struck_on_local_roads
					,sum(case when dcfunctionalclass_desc = 'Minor Arterial' then total_bicyclists else 0 end) as bikers_struck_on_minor_arterials
					,sum(case when dcfunctionalclass_desc = 'Principal Arterial' then total_bicyclists else 0 end) as bikers_struck_on_principal_arterials
					,sum(case when dcfunctionalclass_desc = 'Collector' then total_bicyclists else 0 end) as bikers_struck_on_collectors
				from analysis_data.dc_crashes_w_details 
				where date_part('year', fromdate) between 2015 and 2020
				group by comp_plan_area 
			   ) b on b.compplanarea_extra1= a.comp_plan_area 
) with data;
--select * from comp_plan_4
/*
311 request variables
*/

drop table if exists comp_plan_5;
create temp table comp_plan_5 on commit preserve rows as (
select a.*
	,b.num_311_requests
	,b.num_311_requests_granted
from comp_plan_4 a
left join (select 
		   		comp_plan_area
		   		,count(*) as num_311_requests
		   		,sum(case when cwo_objectid is not null then 1 else 0 end) as num_311_requests_granted
		   	from analysis_data.all311 
			group by comp_plan_area ) b on b.comp_plan_area= a.comp_plan_area 
) with data;

/*
Average roadway variables
*/

drop table if exists comp_plan_6;
create temp table comp_plan_6 on commit preserve rows as (
select a.*
	,b.*
from comp_plan_5 a
left join (select 
		   		comp_plan_area as compplanarea_extra2
		   		,count(*) as total_blocks
		   		,avg(aadt) as avg_aadt
		   		,avg(totalcrosssectionwidth) as avg_road_width
		   		,avg(speed_limit) as avg_speed_limit
		   		,sum(case when dcfunctionalclass_desc = 'Local' then 1 else 0 end) as local_blocks
				,sum(case when dcfunctionalclass_desc = 'Minor Arterial' then 1 else 0 end) as minor_arterial_blocks
				,sum(case when dcfunctionalclass_desc = 'Principal Arterial' then 1 else 0 end) as principal_arterial_blocks
				,sum(case when dcfunctionalclass_desc = 'Collector' then 1 else 0 end) as collector_blocks
		   	from analysis_data.roadway_blocks 
			group by comp_plan_area ) b on b.compplanarea_extra2= a.comp_plan_area 
) with data;

/*
Create final table
*/

drop table if exists viz.comp_plan_area_statistics;
create table viz.comp_plan_area_statistics as 
select * from comp_plan_6;
select comp_plan_area, num_peds_struck, 
peds_struck_on_local_roads, peds_struck_on_minor_arterials+peds_struck_on_principal_arterials as peds_struck_on_arterials, peds_struck_on_collectors
,peds_struck_on_local_roads/(total_pop_2010/1000.0) as peds_struck_on_local_roads_pc
 , (peds_struck_on_minor_arterials+peds_struck_on_principal_arterials)/(total_pop_2010/1000.0) as peds_struck_on_arterials_pc
 , peds_struck_on_collectors/(total_pop_2010/1000.0) as peds_struck_on_collectors_pc
from viz.comp_plan_area_statistics

alter table viz.comp_plan_area_statistics drop column compplanarea_extra2;
alter table viz.comp_plan_area_statistics drop column compplanarea_extra1;

create temp table pulsepoint_anacostia on commit preserve rows as (
select * from source_data.pulsepoint
where ST_Intersects(geography, (select geography from source_data.neighborhood_clusters where nbh_names = 'Historic Anacostia'))
) with data;
select * from pulsepoint_anacostia
where unit_status_transport>0
select sum(unit_status_transport) from pulsepoint_anacostia
--5 

select * from analysis_data.dc_crashes_w_details where nbh_cluster_names = 'Historic Anacostia'
and date_part('year', fromdate) = 2021 and (pedestrian_injuries > 0 or bicycle_injuries>0)

select * from analysis_data.dc_crashes_w_details where nbh_cluster_names = 'Historic Anacostia'
and date_part('year', fromdate) = 2021 and total_injuries > 0
--8 injuries
------------------------------
--Census tract level
------------------------------
/*create boundaries*/
drop table if exists viz.census_tract_boundaries;
create table viz.census_tract_boundaries as
SELECT a.tract as census_tract,
      (ST_Dumppoints(a.geography::geometry)).path[2] as POINT_ORDER
	  ,ST_X((ST_Dumppoints(a.geography::geometry)).geom) as Latitude
	  ,ST_Y((ST_Dumppoints(a.geography::geometry)).geom) as Longitude
FROM source_data.census_tracts a;

/*
Area in km2 
*/

drop table if exists census_tract_area;
create temp table census_tract_area on commit preserve rows as (
select distinct
		tract as census_tract
		,geography
		,st_area(geography::geography)/(1000.0^2) as area_in_km2
		,st_area(geography::geography) as area_in_m2
from source_data.census_tracts
	) with data;

/*
Census variables:
Total population
Households without a car
Federal AGI
*/
drop table if exists census_variables;
create temp table census_variables on commit preserve rows as (
select 
	b.*
	,a.total_pop
	,a.pct_households_w_car
	,a.total_households
	,a.num_households_w_car
	,fagi_total_2015/(total_pop*1.00) as fagi_per_capita_2015
from analysis_data.acs_2019_by_tract a
inner join census_tract_area b on a.census_tract = b.census_tract and a.state = '11'
	) with data;
select * from analysis_data.acs_2019_by_tract
/*
Metro station ridership variables
*/
drop table if exists census_tract_3;
create temp table census_tract_3 on commit preserve rows as (
select a.*
	,cast(b.daily_riders as decimal(10,2)) as daily_riders 
from census_variables a
left join (select 
		   		census_tract
		   		,sum(avg_2015_2018) as daily_riders 
		   	from analysis_data.metro_stations_ridership 
			group by census_tract ) b on b.census_tract= a.census_tract 
) with data;
--select sum(total_pop) from census_tract_3
/*
Car crash variables
*/

drop table if exists census_tract_4;
create temp table census_tract_4 on commit preserve rows as (
select a.*
	,b.*
	from census_tract_3 a
	inner join (select 
					census_tract as census_tract_extra1
					--Pedestrians stats
					,sum(total_pedestrians) as num_peds_struck
					,sum(pedestrian_fatalities) as num_peds_killed
					,sum(peds_under_12) as peds_under_12
					,sum(peds_over_80) as peds_over_80
					,sum(case when total_pedestrians>0 then oos_vehicles else 0 end) as peds_struck_by_oos_drivers
					,sum(case when total_pedestrians>0 then driver_tickets else 0 end) as ped_strike_driver_ticketed
					,sum(case when total_pedestrians>0 then ped_tickets else 0 end) as ped_strike_ped_ticketed
					,avg(case when total_pedestrians>0 then aadt else null end) as avg_aadt_ped_accidents
					,avg(case when total_pedestrians>0 then totalcrosssectionwidth else null end) as avg_road_width_ped_accidents
					,avg(case when total_pedestrians>0 then speed_limit else null end) as avg_speed_limit_ped_accidents
					,sum(case when near_schools[1] is not null then total_pedestrians else 0 end) as peds_struck_near_schools 
					,sum(case when dcfunctionalclass_desc = 'Local' then total_pedestrians else 0 end) as peds_struck_on_local_roads
					,sum(case when dcfunctionalclass_desc = 'Minor Arterial' then total_pedestrians else 0 end) as peds_struck_on_minor_arterials
					,sum(case when dcfunctionalclass_desc = 'Principal Arterial' then total_pedestrians else 0 end) as peds_struck_on_principal_arterials
					,sum(case when dcfunctionalclass_desc = 'Collector' then total_pedestrians else 0 end) as peds_struck_on_collectors
					--Biker stats
					,sum(total_bicyclists) as num_bicyclists_struck
					,sum(bicycle_fatalities) as num_bicyclists_killed
					,sum(bikers_under_18) as bikers_under_18
					,sum(bikers_over_70) as bikers_over_70
					,sum(case when total_bicyclists>0 then oos_vehicles else 0 end) as bikers_struck_by_oos_drivers
					,sum(case when total_bicyclists>0 then driver_tickets else 0 end) as bike_strike_driver_ticketed
					,sum(case when total_bicyclists>0 then ped_tickets else 0 end) as bike_strike_biker_ticketed
					,avg(case when total_bicyclists>0 then aadt else null end) as avg_aadt_bike_accidents
					,avg(case when total_bicyclists>0 then totalcrosssectionwidth else null end) as avg_road_width_bike_accidents
					,avg(case when total_bicyclists>0 then speed_limit else null end) as avg_speed_limit_bike_accidents
					,sum(case when near_schools[1] is not null then total_bicyclists else 0 end) as bikers_struck_near_schools 
					,sum(case when dcfunctionalclass_desc = 'Local' then total_bicyclists else 0 end) as bikers_struck_on_local_roads
					,sum(case when dcfunctionalclass_desc = 'Minor Arterial' then total_bicyclists else 0 end) as bikers_struck_on_minor_arterials
					,sum(case when dcfunctionalclass_desc = 'Principal Arterial' then total_bicyclists else 0 end) as bikers_struck_on_principal_arterials
					,sum(case when dcfunctionalclass_desc = 'Collector' then total_bicyclists else 0 end) as bikers_struck_on_collectors
				from analysis_data.dc_crashes_w_details 
				where date_part('year', fromdate) between 2015 and 2020
				group by census_tract 
			   ) b on b.census_tract_extra1= a.census_tract 
) with data;
--select * from census_tract_4
/*
311 request variables
*/

drop table if exists census_tract_5;
create temp table census_tract_5 on commit preserve rows as (
select a.*
	,b.num_311_requests
	,b.num_311_requests_granted
from census_tract_4 a
left join (select 
		   		census_tract
		   		,count(*) as num_311_requests
		   		,sum(case when cwo_objectid is not null then 1 else 0 end) as num_311_requests_granted
		   	from analysis_data.all311 
			group by census_tract ) b on b.census_tract= a.census_tract 
) with data;

/*
Average roadway variables
*/

drop table if exists census_tract_6;
create temp table census_tract_6 on commit preserve rows as (
select a.*
	,b.*
from census_tract_5 a
left join (select 
		   		census_tract as census_tract_extra2
		   		,count(*) as total_blocks
		   		,avg(aadt) as avg_aadt
		   		,avg(totalcrosssectionwidth) as avg_road_width
		   		,avg(speed_limit) as avg_speed_limit
		   		,sum(case when dcfunctionalclass_desc = 'Local' then 1 else 0 end) as local_blocks
				,sum(case when dcfunctionalclass_desc = 'Minor Arterial' then 1 else 0 end) as minor_arterial_blocks
				,sum(case when dcfunctionalclass_desc = 'Principal Arterial' then 1 else 0 end) as principal_arterial_blocks
				,sum(case when dcfunctionalclass_desc = 'Collector' then 1 else 0 end) as collector_blocks
		   	from analysis_data.roadway_blocks 
			group by census_tract ) b on b.census_tract_extra2= a.census_tract 
) with data;

/*
Create final table
*/

drop table if exists viz.census_tract_statistics;
create table viz.census_tract_statistics as 
select * from census_tract_6;
--select * from viz.census_tract_statistics

alter table viz.census_tract_statistics drop column census_tract_extra2;
alter table viz.census_tract_statistics drop column census_tract_extra1;

------------------------------
--Neighborhood level
------------------------------
select * from source_data.smd_boundaries
/*create boundaries*/
drop table if exists viz.nbh_boundaries;
create table viz.nbh_boundaries as
SELECT a.nbh_names as nbh_cluster_names,
      (ST_Dumppoints(a.geography::geometry)).path[2] as POINT_ORDER
	  ,ST_X((ST_Dumppoints(a.geography::geometry)).geom) as Latitude
	  ,ST_Y((ST_Dumppoints(a.geography::geometry)).geom) as Longitude
FROM source_data.neighborhood_clusters a;

/*
Area in km2 
*/

drop table if exists nbh_cluster_names_area;
create temp table nbh_cluster_names_area on commit preserve rows as (
select 
		nbh_names as nbh_cluster_names
		,geography
		,st_area(geography::geography)/(1000.0^2) as area_in_km2
		,st_area(geography::geography) as area_in_m2
from source_data.neighborhood_clusters
	) with data;

/*
Census variables:
Total population
Households without a car
Federal AGI
*/

drop table if exists nbh_cluster_names_2;
create temp table nbh_cluster_names_2 on commit preserve rows as (
select 
		a.*
		,b.total_pop
		,b.num_households
		,b.num_households_w_car
		,b.num_households_w_car/(b.num_households*1.00) as pct_households_w_car
		,b.fagi_per_capita_2015
from nbh_cluster_names_area a
	inner join (select nbh_cluster_names
				, sum(total_pop) as total_pop
				, sum(total_households) as num_households
				, sum(num_households_w_car) as num_households_w_car
				,sum(fagi_total_2015)/sum(total_pop*1.00) as fagi_per_capita_2015
				from analysis_data.acs_2019_by_tract
				group by nbh_cluster_names) b on b.nbh_cluster_names=a.nbh_cluster_names
	) with data;
--select * from nbh_cluster_names_2 where total_pop is null
/*
Metro station ridership variables
*/
drop table if exists nbh_cluster_names_3;
create temp table nbh_cluster_names_3 on commit preserve rows as (
select a.*
	,cast(b.daily_riders as decimal(10,2)) as daily_riders 
from nbh_cluster_names_2 a
left join (select 
		   		nbh_cluster_names
		   		,sum(avg_2015_2018) as daily_riders 
		   	from analysis_data.metro_stations_ridership 
			group by nbh_cluster_names ) b on b.nbh_cluster_names= a.nbh_cluster_names 
) with data;
--select * from nbh_cluster_names_3 where daily_riders > total_pop
/*
Car crash variables
*/
drop table if exists nbh_cluster_names_4;
create temp table nbh_cluster_names_4 on commit preserve rows as (
select a.*
	,b.*
	from nbh_cluster_names_3 a
	inner join (select 
					nbh_cluster_names as nbh_cluster_names_extra1
					--Pedestrians stats
					,sum(total_pedestrians) as num_peds_struck
					,sum(pedestrian_fatalities) as num_peds_killed
					,sum(peds_under_12) as peds_under_12
					,sum(peds_over_80) as peds_over_80
					,sum(case when total_pedestrians>0 then oos_vehicles else 0 end) as peds_struck_by_oos_drivers
					,sum(case when total_pedestrians>0 then driver_tickets else 0 end) as ped_strike_driver_ticketed
					,sum(case when total_pedestrians>0 then ped_tickets else 0 end) as ped_strike_ped_ticketed
					,avg(case when total_pedestrians>0 then aadt else null end) as avg_aadt_ped_accidents
					,avg(case when total_pedestrians>0 then totalcrosssectionwidth else null end) as avg_road_width_ped_accidents
					,avg(case when total_pedestrians>0 then speed_limit else null end) as avg_speed_limit_ped_accidents
					,sum(case when near_schools[1] is not null then total_pedestrians else 0 end) as peds_struck_near_schools 
					,sum(case when dcfunctionalclass_desc = 'Local' then total_pedestrians else 0 end) as peds_struck_on_local_roads
					,sum(case when dcfunctionalclass_desc = 'Minor Arterial' then total_pedestrians else 0 end) as peds_struck_on_minor_arterials
					,sum(case when dcfunctionalclass_desc = 'Principal Arterial' then total_pedestrians else 0 end) as peds_struck_on_principal_arterials
					,sum(case when dcfunctionalclass_desc = 'Collector' then total_pedestrians else 0 end) as peds_struck_on_collectors
					--Biker stats
					,sum(total_bicyclists) as num_bicyclists_struck
					,sum(bicycle_fatalities) as num_bicyclists_killed
					,sum(bikers_under_18) as bikers_under_18
					,sum(bikers_over_70) as bikers_over_70
					,sum(case when total_bicyclists>0 then oos_vehicles else 0 end) as bikers_struck_by_oos_drivers
					,sum(case when total_bicyclists>0 then driver_tickets else 0 end) as bike_strike_driver_ticketed
					,sum(case when total_bicyclists>0 then ped_tickets else 0 end) as bike_strike_biker_ticketed
					,avg(case when total_bicyclists>0 then aadt else null end) as avg_aadt_bike_accidents
					,avg(case when total_bicyclists>0 then totalcrosssectionwidth else null end) as avg_road_width_bike_accidents
					,avg(case when total_bicyclists>0 then speed_limit else null end) as avg_speed_limit_bike_accidents
					,sum(case when near_schools[1] is not null then total_bicyclists else 0 end) as bikers_struck_near_schools 
					,sum(case when dcfunctionalclass_desc = 'Local' then total_bicyclists else 0 end) as bikers_struck_on_local_roads
					,sum(case when dcfunctionalclass_desc = 'Minor Arterial' then total_bicyclists else 0 end) as bikers_struck_on_minor_arterials
					,sum(case when dcfunctionalclass_desc = 'Principal Arterial' then total_bicyclists else 0 end) as bikers_struck_on_principal_arterials
					,sum(case when dcfunctionalclass_desc = 'Collector' then total_bicyclists else 0 end) as bikers_struck_on_collectors
				from analysis_data.dc_crashes_w_details 
				where date_part('year', fromdate) between 2015 and 2020
				group by nbh_cluster_names 
			   ) b on b.nbh_cluster_names_extra1= a.nbh_cluster_names 
) with data;
--select * from nbh_cluster_names_4
/*
311 request variables
*/

drop table if exists nbh_cluster_names_5;
create temp table nbh_cluster_names_5 on commit preserve rows as (
select a.*
	,b.num_311_requests
	,b.num_311_requests_granted
from nbh_cluster_names_4 a
left join (select 
		   		nbh_cluster_names
		   		,count(*) as num_311_requests
		   		,sum(case when cwo_objectid is not null then 1 else 0 end) as num_311_requests_granted
		   	from analysis_data.all311 
			group by nbh_cluster_names ) b on b.nbh_cluster_names= a.nbh_cluster_names 
) with data;

/*
Average roadway variables
*/

drop table if exists nbh_cluster_names_6;
create temp table nbh_cluster_names_6 on commit preserve rows as (
select a.*
	,b.*
from nbh_cluster_names_5 a
left join (select 
		   		nbh_cluster_names as nbh_cluster_names_extra2
		   		,count(*) as total_blocks
		   		,avg(aadt) as avg_aadt
		   		,avg(totalcrosssectionwidth) as avg_road_width
		   		,avg(speed_limit) as avg_speed_limit
		   		,sum(case when dcfunctionalclass_desc = 'Local' then 1 else 0 end) as local_blocks
				,sum(case when dcfunctionalclass_desc = 'Minor Arterial' then 1 else 0 end) as minor_arterial_blocks
				,sum(case when dcfunctionalclass_desc = 'Principal Arterial' then 1 else 0 end) as principal_arterial_blocks
				,sum(case when dcfunctionalclass_desc = 'Collector' then 1 else 0 end) as collector_blocks
		   	from analysis_data.roadway_blocks 
			group by nbh_cluster_names ) b on b.nbh_cluster_names_extra2= a.nbh_cluster_names 
) with data;

/*
Create final table
*/

select * from viz.nbh_cluster_names_statistics

drop table if exists viz.nbh_cluster_names_statistics;
create table viz.nbh_cluster_names_statistics as 
select * from nbh_cluster_names_6;
--select * from source_data.neighborhood_clusters order by nbh_names
--46
--select * from viz.nbh_cluster_names_statistics order by num_peds_struck  where daily_riders<(1.5*total_pop_2010) or daily_riders is null
--select * from viz.nbh_cluster_names_statistics 
select * from analysis_data.dc_crashes_w_details where (nbh_cluster_names ilike '%randle%' or nbh_cluster_names ilike '%naylor%') 
														and date_part('year', fromdate) between 2018 and 2020
and total_pedestrians>0
select * from source_data.pulsepoint where agency_id = '05900'
select * from analysis_data.dc_crashes_w_details where (nbh_cluster_names ilike '%georgetown%' ) 
														and date_part('year', fromdate) between 2018 and 2020
and total_pedestrians>0 and oos_vehicles > 0

select * from analysis_data.dc_crashes_w_details where (nbh_cluster_names ilike '%congress heights%' or nbh_cluster_names ilike '%anacostia%' or 
nbh_cluster_names ilike '%shipley terrace%' or nbh_cluster_names ilike '%woodland%') and date_part('year', fromdate) between 2018 and 2020
and total_pedestrians>0 and oos_vehicles=0

select * from analysis_data.dc_crashes_w_details where (nbh_cluster_names ilike '%tenleytown%' or nbh_cluster_names ilike '%chevy chase%' or 
nbh_cluster_names ilike '%shepherd park%' or nbh_cluster_names ilike '%palisades%') and date_part('year', fromdate) between 2015 and 2020
and total_pedestrians>0
select sum(total_pop_2019), sum(total_pop_2010)from viz.nbh_cluster_names_statistics 
--688761	596921
where daily_riders<(1.5*total_pop_2010) or daily_riders is null
--623118	544768
select * from analysis_data.acs_2019_by_tract where nbh_cluster_names ilike '%National Mall, Potomac River%'
select * from analysis_data.acs_2019_by_tract where nbh_cluster_names ilike '%National Mall, Potomac River%'
--where  census_tract = '010900'
--update analysis_data.acs_2019_by_tract set nbh_cluster_names = 'Woodridge, Fort Lincoln, Gateway', name = 'Cluster 24' where census_tract = '011100'
--update analysis_data.acs_2019_by_tract set nbh_cluster_names = 'Ivy City, Arboretum, Trinidad, Carver Langston' where nbh_cluster_names = 'Arboretum, Anacostia River'
--update analysis_data.acs_2019_by_tract set nbh_cluster_names = 'Takoma, Brightwood, Manor Park' where census_tract = '001803'
--update analysis_data.acs_2019_by_tract set nbh_cluster_names = 'North Cleveland Park, Forest Hills, Van Ness' where census_tract = '001301'
alter table viz.nbh_cluster_names_statistics drop column nbh_cluster_names_extra2;
alter table viz.nbh_cluster_names_statistics drop column nbh_cluster_names_extra1;

select * from 
------------------------------
--ANC level
------------------------------

------------------------------
--Ward level
------------------------------

------------------------------
--SMD level
------------------------------

drop table if exists viz.smd_boundaries;
create table viz.smd_boundaries as
SELECT a.smd_id as SMD_ID,
      (ST_Dumppoints(a.geography::geometry)).path[2] as POINT_ORDER
	  ,ST_X((ST_Dumppoints(a.geography::geometry)).geom) as Latitude
	  ,ST_Y((ST_Dumppoints(a.geography::geometry)).geom) as Longitude
FROM source_data.smd_boundaries a;

select * from viz.smd_boundaries limit 100;
select * from source_data.anc_boundaries;

drop table if exists viz.anc_boundaries;
create table viz.anc_boundaries as
SELECT a.anc_id as ANC_ID,
      (ST_Dumppoints(a.geography::geometry)).path[2] as POINT_ORDER
	  ,ST_X((ST_Dumppoints(a.geography::geometry)).geom) as Latitude
	  ,ST_Y((ST_Dumppoints(a.geography::geometry)).geom) as Longitude
FROM source_data.anc_boundaries a;

select * from source_data.census_tracts   
select sum(total_pop)from source_data.census_tracts   
select * from source_data.acs_2019_by_tract  where state = '11' order by total_pop
select sum(total_pop) from source_data.acs_2019_by_tract  where state = '11' order by total_pop

select * from source_data.comp_plan_areas

select tract, total_pop, total_households, (1-(total_households_w_no_vehicle/total_households*1.00)) as pct_w_car
from source_data.acs_2019_by_tract 
where  tract = '001401'

select tract, total_pop, total_households, (1-(total_households_w_no_vehicle/total_households*1.00)) as pct_w_car
from source_data.acs_2019_by_tract 
where state = '11' and total_households>0
--group by tract, total_pop, total_households
order by 4 desc 



drop table if exists viz.peds_struck_by_census_tract;
create table viz.peds_struck_by_census_tract as
select census_tract, sum(total_pedestrians) as pedestrians_struck, sum(pedestrian_fatalities ) as pedestrians_killed
from analysis_data.dc_crashes_w_details
where date_part('year', fromdate) between 2015 and 2020
group by census_tract order by 2 desc ;

drop table if exists viz.census_tract_areas;
create table viz.census_tract_areas as
SELECT a.tract as tract, 
      st_area(geography::geography)/(1000.0^2) as area_in_km2
FROM source_data.census_tracts a;

select * from viz.anc_boundaries limit 100;

select * from analysis_data.dc_crashes_w_details limit 100;
select * from analysis_data.all311 limit 100;
select * from source_data.pulsepoint;

select * from source_data.metro_stations_daily_ridership

drop table if exists census_blocks_w_smd;
create temp table census_blocks_w_smd on commit preserve rows as (
select b.smd_id as smd_id
	,row_number() over (partition by a.objectid) as row_num
	,a.*
from source_data.census_blocks a
inner join source_data.smd_boundaries b on st_intersects(a.geography::geometry, b.geography::geometry)
) with data;
--12184
delete from census_blocks_w_smd where row_num>1;
--DELETE 5678
select count(*) from source_data.census_blocks
--ok so i could get one walkability score per census block outside the CBD basically
where total_pop > 0 
--or i could get walkability score of all the pedestrian crashes
drop table if exists smd_pop;
create temp table smd_pop on commit preserve rows as (
select smd_id, sum(total_pop) as total_pop from census_blocks_w_smd
group by smd_id 
	) with data;

drop table if exists viz.safety311_requests_by_smd;
create table viz.safety311_requests_by_smd as
select a.smd_id, count(distinct a.objectid) as total_requests, b.total_pop
from analysis_data.all311 a
inner join smd_pop b on a.smd_id = b.smd_id
group by a.smd_id, b.total_pop order by 2 desc ;

select * from viz.safety311_requests_by_smd order by total_pop asc;

drop table if exists viz.peds_struck_by_smd;
create table viz.peds_struck_by_smd as
select smd_id, sum(total_pedestrians) as pedestrians_struck
from analysis_data.dc_crashes_w_details
where date_part('year', fromdate) between 2015 and 2020
group by smd_id order by 2 desc ;

drop table if exists viz.peds_struck_by_anc;
create table viz.peds_struck_by_anc as
select anc_id, sum(num_pedestrians) as pedestrians_struck
from analysis_data.dc_crashes_w_details
where date_part('year', from_date) between 2015 and 2020
group by anc_id order by 2 desc ;

drop table if exists viz.car_ownership;
create table viz.car_ownership as 
select COMP_PLAN_AREA , VEHICLE_AVAIL_OCCUP_HU_NONE as Num_Households_No_Car, cast(1.00-(VEHICLE_AVAIL_OCCUP_HU_NONE/VEHICLE_AVAIL_OCCUP_HU) as decimal(10,2)) as pct_with_car
from source_data.acs_housing_2011_2015
order by 2 desc;

select * from viz.car_ownership;

update viz.car_ownership
set COMP_PLAN_AREA = 'CAPITOL HILL' where COMP_PLAN_AREA = 'CAPTIOL HILL'

select * from viz.car_ownership;

create temp table census_blocks_w_comp_plan_area on commit preserve rows as (
select b.name as comp_plan_area
	,row_number() over (partition by a.objectid) as row_num
	,a.*
from source_data.census_blocks a
inner join source_data.comp_plan_areas b on st_intersects(a.geography::geometry, b.geometry::geometry)
) with data;

delete from census_blocks_w_comp_plan_area where row_num>1;

drop table if exists comp_plan_pop;
create temp table comp_plan_pop on commit preserve rows as (
select comp_plan_area, sum(total_pop) as total_pop from census_blocks_w_comp_plan_area
group by comp_plan_area 
	) with data;
select * from comp_plan_pop


select * from source_data.comp_plan_areas;
select * from comp_plan_area
	
drop table if exists viz.comp_plan_pop_density;
create table viz.comp_plan_pop_density as (
select a.*, b.area_in_km2, (a.total_pop*1.00)/b.area_in_km2 as pop_per_km2
from comp_plan_pop a
inner join comp_plan_area b on a.comp_plan_area = b.name
	)
	
	select * from viz.comp_plan_pop_density;

create temp table crashes_w_census_blocks on commit preserve rows as (
select b.name as comp_plan_area
--	,c.total_pop
--	,c.objectid as census_objectid
--	, case when c.total_pop <=10 then 'Under 10' else 'Over 10' end as census_block_pop
	,row_number() over (partition by a.objectid) as row_num
	,a.*
from analysis_data.dc_crashes_w_details a
inner join source_data.comp_plan_areas b on st_intersects(a.geography::geometry, b.geometry::geometry)
--left join source_data.census_blocks c on  st_intersects(a.geography::geometry, c.geography::geometry)
where date_part('year',a.fromdate) between 2015 and 2020
) with data;

delete from crashes_w_census_blocks where row_num >1;

select sum(total_pedestrians) from crashes_w_census_blocks
--6637 

drop table if exists viz.ped_crashes;
create table viz.ped_crashes as 
select 
	a.comp_plan_area
	,b.total_pop
	,c.Num_Households_No_Car
	,sum(total_pedestrians) as Num_Pedestrians_Struck
from crashes_w_census_blocks a
inner join comp_plan_pop b on a.comp_plan_area = b.comp_plan_area
inner join viz.car_ownership c on a.comp_plan_area = c.comp_plan_area
group by a.comp_plan_area,b.total_pop,c.Num_Households_No_Car

select * from viz.ped_crashes;
select * from comp_plan_pop;
select sum(num_pedestrians_struck) from viz.ped_crashes;

select * from censustr

select *, (Num_Pedestrians_Struck*1.00)/Num_Households_No_Car
from viz.ped_crashes order by 5 desc 

CREATE ROLE 