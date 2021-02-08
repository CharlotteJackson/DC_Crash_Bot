select nbh_cluster_names, count(*)
from analysis_data.all311 
where date_part('year', adddate)>=2018
group by nbh_cluster_names order by count(*) desc

create temp table anacostia_crashes on commit preserve rows as (
select * from analysis_data.dc_crashes_w_details 
where nbh_cluster_names = 'Historic Anacostia' and date_part('year', fromdate)>=2015
) with data;
--1715 rows 

create temp table pulsepoint_anacostia on commit preserve rows as (
select * from source_data.pulsepoint
where ST_Intersects(geography, (select geography from source_data.neighborhood_clusters where nbh_names = 'Historic Anacostia'))
) with data;
--15 rows 
select * from pulsepoint_anacostia

alter table source_data.pulsepoint_stream add column AGENCY_ID VARCHAR NULL
update source_data.pulsepoint_stream set AGENCY_ID = 'EMS1205' where AGENCY_ID is null;

select * from source_data.pulsepoint_stream order by scrape_datetime desc

SET TIMEZONE TO 'America/New_York';

select count(*) from source_data.pulsepoint_stream where AGENCY_ID <> 'EMS1205'

select * from source_data.pulsepoint where AGENCY_ID ='05900'

drop table if exists anacostia_311;
create temp table anacostia_311 on commit preserve rows as (
select * from analysis_data.all311 
where nbh_cluster_names = 'Historic Anacostia'
) with data;
--142 rows 
select * from anacostia_311

create temp table anacostia_moving_violations on commit preserve rows as (
select ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography, a.* 
from source_data.moving_violations a
where ST_Intersects(ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography, (select geography from source_data.neighborhood_clusters where nbh_names = 'Historic Anacostia'))
) with data;
--620 rows

