/********************************
Intro the Plsepoint data
*******************************/

select ST_SetSRID(geography, 4326)::geography,* from source_data.census_blocks
where total_pop >=20

select total_pop, count(*) from analysis_data.census_block_level_crashes
where year = 2020
group by total_pop order by total_pop

select ST_SetSRID(geometry, 4326)::geography,* from analysis_data.census_block_level_crashes 
where total_pop >=20 and year = 2020


select ward_id, sum(total_pedestrians)
from analysis_data.census_block_level_crashes a
inner join source_data.ward_boundaries b on st_intersects(ST_SetSRID(a.geometry, 4326)::geometry, b.geography::geometry)
where year between 2015 and 2020
and total_pop >=20
group by ward_id order by sum(total_pedestrians) desc 

select b.name, sum(total_pedestrians+total_bicyclists)
from analysis_data.census_block_level_crashes a
inner join source_data.comp_plan_areas b on st_intersects(ST_SetSRID(a.geometry, 4326)::geometry, b.geometry::geometry)
where year between 2015 and 2020
--and total_pop >=20
group by b.name order by 2 desc 

create temp table crashes_w_census_blocks on commit preserve rows as (
select b.name as comp_plan_area
	,c.total_pop
	,c.objectid as census_objectid
	, case when c.total_pop <=10 then 'Under 10' else 'Over 10' end as census_block_pop
	,row_number() over (partition by a.objectid) as row_num
	,a.*
from analysis_data.dc_crashes_w_details a
inner join source_data.comp_plan_areas b on st_intersects(a.geography::geometry, b.geometry::geometry)
inner join source_data.census_blocks c on  st_intersects(a.geography::geometry, c.geography::geometry)
where date_part('year',a.fromdate) between 2015 and 2020
) with data;
--SELECT 143713
select count(*) from analysis_data.dc_crashes_w_details
where date_part('year',fromdate) between 2015 and 2020
--147043
select * from crashes_w_census_blocks where row_num >1;
delete from  crashes_w_census_blocks where row_num >1;

select comp_plan_area, census_block_pop,count(distinct objectid)
from crashes_w_census_blocks
where (total_pedestrians>0)
group by comp_plan_area,census_block_pop order by comp_plan_area,census_block_pop

select comp_plan_area, sum(total_pedestrians)
from crashes_w_census_blocks
where (total_pedestrians>0) and date_part('year', fromdate) = 2020
group by comp_plan_area order by 2 desc

select comp_plan_area, sum(total_pedestrians)
from crashes_w_census_blocks
where total_pedestrians>0 and total_pop>0
group by comp_plan_area order by 2 desc

SELECT a.*,
      (ST_Dumppoints(a.geometry::geometry)).path, ST_AsText((ST_Dumppoints(a.geometry::geometry)).geom) AS geo_dump
FROM source_data.comp_plan_areas a

SELECT a.*,
      (ST_Dumppoints(a.geometry::geometry)).path[2] as point_number
	  , ST_AsText((ST_Dumppoints(a.geometry::geometry)).geom) AS geo_dump
	  ,ST_X((ST_Dumppoints(a.geometry::geometry)).geom) as Latitude
	  ,ST_Y((ST_Dumppoints(a.geometry::geometry)).geom) as Longitude
FROM source_data.comp_plan_areas a

"geo_dump"
"POINT(-77.052438121501 38.9870111137835)"
"geo_dump"
"POINT(-77.052438121501 38.9870111137835)"

select * from source_data.comp_plan_areas

select comp_plan_area , cast(1.00-(VEHICLE_AVAIL_OCCUP_HU_NONE/VEHICLE_AVAIL_OCCUP_HU) as decimal(10,2)) as pct_with_car
from source_data.acs_housing_2011_2015
order by 2 desc

select ward_name, count(distinct objectid)
from analysis_data.dc_crashes_w_details
where cast(fromdate as date) between '2015-01-01' and '2021-01-18'
and (total_bicyclists>0 or total_pedestrians>0 or (vehicle_injuries>0 and oos_vehicles<total_vehicles))
group by ward_name order by count(distinct objectid) desc 

select * from source_data.pulsepoint_stream where cast(call_received_datetime as date) >='2020-12-30'
order by incident_id, scrape_datetime;
select * from source_data.pulsepoint where cast(call_received_datetime as date) >='2020-12-30'
order by incident_id, scrape_datetime;
select * from source_data.pulsepoint;

select max(fromdate) from analysis_data.dc_crashes_w_details;
select objectid from  analysis_data.dc_crashes_w_details group by objectid having count(*)>1;
--limit crashes to just the dates in the pulsepoint data
drop table if exists tmp_crashes;
create temp table tmp_crashes on commit preserve rows as (
select distinct a.* 
	from analysis_data.dc_crashes_w_details a
	where cast(fromdate as date) between '2020-12-30' and '2021-01-10'
	) with data;
--492 records 
--now its 514?...
--select * from tmp_crashes

DROP TABLE IF EXISTS temp_pulsepoint;
CREATE TEMP TABLE temp_pulsepoint on commit preserve rows
AS (

SELECT 
	c.anc_id
	,c.geography as anc_boundary
	,d.name as nbh_cluster
	,d.nbh_names as nbh_cluster_names
	,d.geography as nbh_cluster_boundary
	,e.smd_id
	,e.geography as smd_boundary
	,f.name as ward_name 
	,f.geography as ward_boundary
    ,ROW_NUMBER() OVER (PARTITION BY a.incident_id) as ROW_NUM
	,a.*
FROM source_data.pulsepoint a
LEFT JOIN tmp.anc_boundaries c ON ST_Intersects(c.geography::geometry, a.geography::geometry)
LEFT JOIN tmp.neighborhood_clusters d ON ST_Intersects(d.geography::geometry, a.geography::geometry)
LEFT JOIN tmp.smd_boundaries e ON ST_Intersects(e.geography::geometry, a.geography::geometry)
LEFT JOIN tmp.ward_boundaries f ON ST_Intersects(f.geography::geometry, a.geography::geometry)
) WITH DATA ;
--670

select * from source_data.census_blocks where total_pop<=5;

--select * from temp_pulsepoint where row_num>1;
--0
create table tmp.dcfems_vs_mpd_sample as select * from temp_join;
grant all privileges on table tmp.dcfems_vs_mpd_sample to public;
--join crashes and pulsepoint
DROP TABLE IF EXISTS temp_join;
CREATE TEMP TABLE temp_join on commit preserve rows as (
SELECT distinct
	case when objectid is null then 'DCFEMS Only'
		when incident_id is null then 'MPD Only'
		when objectid is not null and incident_id is not null then 'Overlap'
	end as Crash_Category
	,pp.incident_id
	,pp.call_received_datetime
	,pp.call_closed_datetime
	,pp.fulldisplayaddress
	,pp.latitude as pp_latitude
	,pp.longitude as pp_longitude
	,pp.incident_type
	,pp.unit_status_transport
	,pp.transport_unit_is_amr
	,pp.transport_unit_is_non_amr
	,pp.num_units_responding
	,pp.geography as pp_geography
	,ST_Distance(pp.geography,crash.geography) AS distance_between_events
	,pp.unit_status_transport as Total_Injuries_PP
	,pp.anc_id as ANC_PP
	,pp.nbh_cluster_names as nbh_cluster_names_pp
	,pp.ward_name as ward_name_pp
	,case when crash.objectid is null or crash.total_injuries < unit_status_transport then unit_status_transport-coalesce(crash.total_injuries,0) end as Extra_Injuries_PP
	,case when crash.objectid is null or crash.total_major_injuries < transport_unit_is_non_amr 
			then transport_unit_is_non_amr-coalesce(crash.total_major_injuries,0) end as Extra_Major_Injuries_PP
	,case when crash.objectid is null or crash.total_minor_injuries < transport_unit_is_amr 
			then transport_unit_is_amr-coalesce(crash.total_minor_injuries,0) end as Extra_Minor_Injuries_PP
	,crash.*
FROM (select * from temp_pulsepoint where cast(CALL_RECEIVED_DATETIME as date) between '2020-12-30' and '2021-01-10') pp 
FULL OUTER JOIN tmp_crashes crash ON ST_DWITHIN(ST_Force2D(crash.geography::geometry), pp.geography, 150)--reported locations within 150 meters of each other?
	AND cast(fromdate as date) =cast(CALL_RECEIVED_DATETIME as date)--fromdate doesn't have a timestamp, so same date is the most granular possible join
	AND reportdate >= call_received_datetime --but dont want to match records that were reported on prior to the call coming in
) WITH DATA;
--SELECT 625
--now 645
--select * from temp_join;
--select count(distinct incident_id) from temp_join;
--246
--select count(distinct incident_id) from temp_join where objectid is not null;
--118 

select crash_category, count(*) from temp_join
group by crash_category order by count(*) desc 
/*
"crash_category"	"count"
"MPD Only"	390
"Overlap"	129
"DCFEMS Only"	126
*/
--rows where one pulsepoint call matched to multiple MPD records
select * from temp_join where incident_id in (select incident_id from temp_join group by incident_id having count(distinct objectid)>1)
order by incident_id, objectid 
--9 calls each matched to 2 crashes

--rows where one MPD record matched multiple PP calls
select * from temp_join where objectid in (select objectid from temp_join group by objectid having count(distinct incident_id)>1)
order by incident_id, objectid 
--9 calls each matched to 2 crashes

--check out the records that have been matched
drop table if exists stacked_matches;
create temp table stacked_matches on commit preserve rows as (
select incident_id, objectid, distance_between_events, 'pulsepoint' as geo_type, pp_geography as geography
	from temp_join where incident_id is not null and objectid is not null
union all 
select incident_id, objectid, distance_between_events, 'mpd' as geo_type, geography as geography
	from temp_join where incident_id is not null and objectid is not null
) with data;

select * from stacked_matches order by distance_between_events desc,geo_type

select * from temp_join where objectid = '183685663'

select * from temp_join
where objectid is null

select * from temp_join 
where Extra_Injuries_PP >0

select * from source_data.crashes_raw limit 100

select * from temp_join where unit_status_transport>0 and objectid is null

select 
	min(coalesce(ward_name_pp,ward_name)) as Ward
	,coalesce(nbh_cluster_names_pp,nbh_cluster_names) as Neighborhood
	,count(distinct case when crash_category = 'DCFEMS Only' then incident_id else null end) as Crashes_DCFEMS_Only
	,count(distinct case when crash_category = 'MPD Only' then objectid else null end) as Crashes_MPD_Only
	,count(distinct case when crash_category = 'Overlap' then objectid else null end) as Overlap
	,sum(total_injuries) as MPD_Reported_Injuries
	,sum(Extra_Injuries_PP) as DCFEMS_Injuries_Not_In_MPD_Reports
from temp_join
group by coalesce(nbh_cluster_names_pp,nbh_cluster_names) 
order by count(distinct objectid) desc

select 
	coalesce(ward_name_pp,ward_name) as Ward
	,count(distinct objectid) as MPD_Reported_Crashes
	,count(distinct Extra_Crash_PP) as DCFEMS_Crashes_Not_In_MPD_Reports
	,sum(total_injuries) as MPD_Reported_Injuries
	,sum(Extra_Injuries_PP) as DCFEMS_Injuries_Not_In_MPD_Reports
from temp_join
group by coalesce(ward_name_pp,ward_name)
order by count(distinct objectid) desc

select 
	coalesce(ward_name_pp,ward_name) as Ward
	,count(distinct case when objectid is not null and incident_id is null then objectid else null end) as MPD_Reported_Crashes_Not_In_DCFEMS_Data
	,count(distinct case when objectid is not null and incident_id is not null then objectid else null end) as Overlapping_Crashes
	,count(distinct Extra_Crash_PP) as DCFEMS_Crashes_Not_In_MPD_Reports
	,sum(total_injuries) as MPD_Reported_Injuries
	,sum(Extra_Injuries_PP) as DCFEMS_Injuries_Not_In_MPD_Reports
from temp_join
group by coalesce(ward_name_pp,ward_name)
order by count(distinct objectid) desc

select * from tmp.twitter