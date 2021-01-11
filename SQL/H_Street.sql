/********************************
QUESTIONS: 
    Did the streetcar have an impact on accident rates?
	How often is speeding a factor?
	Which intersections are problem points?
Datasets used to find the answer: 
    select * from analysis_data.dc_crashes_w_details limit 1000: Data on car crashes from Open Data DC, with some columns added for easier analysis
	select *,ST_Force2D(geometry::geometry)::geography  from source_data.roadway_blocks  where routename in ('H ST NE', 'BENNING RD NE')
	select *,ST_Force2D(geometry::geometry)::geography from source_data.roadway_intersection_approach where routename in ('H ST NE', 'BENNING RD NE')
PostGIS functions used to find the answer:
    ST_ConcaveHull
    ST_Collect
    ST_Intersects
	ST_DWithin
*******************************/
--Data exploration
select *,ST_Force2D(geometry::geometry)::geography
from  source_data.roadway_intersection_approach
where intersectionid = '12000602_12042442'

drop table if exists intersection_polygons;
create temp table intersection_polygons on commit preserve rows as (
select intersectionid, array_agg(distinct routename) as int_name, ST_ConcaveHull(ST_Collect(ST_Force2D(geometry)), 0.99)::geography AS geography
from source_data.roadway_intersection_approach
group by intersectionid
	) with data;
	
select * from intersection_polygons 
select * from source_data.crash_details limit 100;
select * from source_data.crashes_raw 
where speeding_involved>0 and date_part('year', reportdate)>=2015
limit 100;

drop table if exists h_st_crashes;
create temp table h_st_crashes on commit preserve rows as (
select distinct a.* , b.block_name
from analysis_data.dc_crashes_w_details a
inner join source_data.roadway_blocks b on ST_DWithin(ST_Force2D(b.geometry::geometry)::geography,a.geography,30)
where  b.routename in ('H ST NE', 'BENNING RD NE') 
and ST_DWithin(a.geography::geography, '0101000020E6100000030F554C994053C029DFF3583A734340'::geography,2305)
	and date_part('year',fromdate)>=2015
) with data;

--add intersection id and name 
drop table if exists h_st_intersections;
create temp table h_st_intersections on commit preserve rows as (
select distinct a.* , c.int_name, c.intersectionid, c.geography as int_polygon
from h_st_crashes a
left join intersection_polygons c on ST_Intersects(a.geography::geometry,c.geography::geometry)
) with data;

select 

select 
	CONCAT(date_part('year', fromdate),'-', right(concat('0',date_part('month', fromdate)),2)) 
	,count(*) as total_crashes
	,sum(total_injuries) as total_injuries
	,sum(bicycle_fatalities+pedestrian_fatalities+vehicle_fatalities) as total_fatalities
	,sum(total_bicyclists) as num_bikers
	,sum(bicycle_injuries) as bike_injuries
	,sum(bicycle_fatalities) as bicycle_fatalities
	,sum(total_pedestrians) as num_pedestrians
	,sum(pedestrian_injuries) as pedestrian_injuries
	,sum(pedestrian_fatalities) as pedestrian_fatalities
from h_st_intersections
group by CONCAT(date_part('year', fromdate),'-', right(concat('0',date_part('month', fromdate)),2)) 
order by CONCAT(date_part('year', fromdate),'-', right(concat('0',date_part('month', fromdate)),2)) 

select *
from h_st_intersections
where int_name is not null;

select int_name
	, count(*) as total_crashes
	, sum(total_pedestrians) as num_pedestrians
	, sum(total_bicyclists) as num_bikes
	, sum(total_vehicles) as num_cars
	, 
from h_st_intersections
group by int_name order by count(*) desc 

select 
objectid
,reportdate
,fromdate
,address
,bicycle_injuries
,vehicle_injuries
,pedestrian_injuries
,total_injuries
,total_major_injuries
,total_minor_injuries
,bicycle_fatalities
,pedestrian_fatalities
,vehicle_fatalities
,drivers_impaired
,drivers_speeding
,total_vehicles
,total_bicyclists
,total_pedestrians
,drivers_over_80
,drivers_under_25
,peds_over_70
,peds_under_12
,bikers_over_70
,oos_vehicles
,num_cars
,num_suvs_or_trucks
,driver_tickets
,bicycle_tickets
,ped_tickets
,persontype_array
,invehicletype_array
,licenseplatestate_array
,intapproachdirection
,block_name
,int_name
,intersectionid
from h_st_intersections;

select count(*) from h_st_crashes;
--2308

select * 
from h_st_crashes
where driver_tickets>0 or drivers_speeding>0
;
objectid 18209448 --easternmost
objectid 182182116 --westernmost
--"geography"
--"0101000020E6100000030F554C994053C029DFF3583A734340"

select a.*, b.geography as easternmost_crash, ST_Distance(a.geography::geography,b.geography::geography) as max_distance 
from  analysis_data.dc_crashes_w_details a
inner join analysis_data.dc_crashes_w_details b on 1=1
where a.objectid = '182182116' and b.objectid = '182123545'
