select nbh_cluster_names, count(*)
from analysis_data.all311 
where date_part('year', adddate)>=2018
group by nbh_cluster_names order by count(*) desc

create temp table anacostia_311 on commit preserve rows as (
select * from analysis_data.all311 
where nbh_cluster_names = 'Historic Anacostia'
) with data;
--142 rows 
select * from anacostia_311
where servicecode in ('MARKINST','S0376','SCCRGUPR','MARKMAIN')
and details not like '%installed%' and details not like '%INSTALLED%' and details not like '%Approv%' and details <> 'CROSSWALK+HAS+BEEN+RE-STRIPPED.+'
and details <> 'CROSSWALK HAS BEEN RE-STRIPPED.'
--servicerequestid? "16-00609101"
select * from source_data.cityworks_service_requests where requestid like '%00609101%'
--69 rows
--so, how many of these got done? does this info live in the work orders table?
select * from work_orders_anacostia where description like '%NEW INSTALL%' and projectname = 'SERVICE REQUESTS'
--i see nothing about servicerequestid :( 
--sourceworkorderid, workorderid, projectid? 
select details, geography from anacostia_311

create temp table anacostia_crashes on commit preserve rows as (
select * from analysis_data.dc_crashes_w_details 
where nbh_cluster_names = 'Historic Anacostia' and date_part('year', fromdate)>=2015
) with data;
--1715 rows 

select * from anacostia_crashes
where total_pedestrians > 0

create temp table pulsepoint_anacostia on commit preserve rows as (
select * from source_data.pulsepoint
where ST_Intersects(geography, (select geography from source_data.neighborhood_clusters where nbh_names = 'Historic Anacostia'))
) with data;
--13 rows 

create temp table work_orders_anacostia on commit preserve rows as (
select a.* 
from source_data.cityworks_work_orders a
where ST_Intersects(a.geography, (select geography from source_data.neighborhood_clusters where nbh_names = 'Historic Anacostia'))
and date_part('year', initiateddate)>=2015
) with data;
--2970 rows

create temp table anacostia_moving_violations on commit preserve rows as (
select ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography, a.* 
from source_data.moving_violations a
where ST_Intersects(ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography, (select geography from source_data.neighborhood_clusters where nbh_names = 'Historic Anacostia'))
) with data;
--620 rows


select 
	description
	,workordercategory
	,projectname
	,count(*)
from source_data.cityworks_work_orders
where date_part('year', initiateddate) >=2015
and description not in ('PLANTING', 'PRUNING', 'PRUNING (ELM)', 'TREE REMOVAL')
group by 
description
	,workordercategory
	,projectname
order by count(*) desc 

select * from source_data.address_points where stname = 'GOOD HOPE' and addrnum like '11%'

select * from source_data.address_points where xcoord = '400901.8' and ycoord = '133403.82'

select * from source_data.neighborhood_clusters
where nbh_names = 'Historic Anacostia'

select * from source_data.address_points where objectid_12 = '809029' or site_address_pk = '809029' or address_id = '809029'
or objectid_1 = '809029.0'