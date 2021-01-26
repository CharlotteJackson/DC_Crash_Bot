
--Create assessment neighborhood (more granular than neighborhood cluster)
DROP TABLE IF EXISTS assessment_nabe;
CREATE TEMP TABLE assessment_nabe ON COMMIT PRESERVE ROWS AS (
SELECT 
    d.assessment_nbhd AS assessment_nabe,
	ST_ConcaveHull(ST_Collect(d.geography::geometry), 0.99) AS geography
FROM source_data.address_points AS d
GROUP BY d.assessment_nbhd
)
WITH DATA;
--SELECT * FROM assessment_nabe;
--"Since there is already signage at this location and motorists are choosing to ignore it
--%2c this then becomes a TRAFFIC ENFORCEMENT issue for which the Metropolitan Police Department (MPD) is the authoritative agency. 
--For DDOT related issues%2c citizen may contact Clearinghouse at 202-671-2700."

--Add ward, neighborhood cluster, ANC, SMD, and assessment neighborhood to crashes
--Use subdivide function to speed up query
DROP TABLE IF EXISTS crashes_w_neighborhood;
CREATE TEMP TABLE crashes_w_neighborhood  ON COMMIT PRESERVE ROWS AS (
	WITH --assessment_nabe2 as (SELECT assessment_nabe, ST_SUBDIVIDE(geography::geometry) geography FROM assessment_nabe),
		anc_boundaries as (SELECT anc_id, ST_SUBDIVIDE(geography::geometry) geography FROM source_data.anc_boundaries),
		neighborhood_clusters as (SELECT name, nbh_names, ST_SUBDIVIDE(geography::geometry) geography FROM source_data.neighborhood_clusters),
		smd_boundaries as (SELECT smd_id, ST_SUBDIVIDE(geography::geometry) geography FROM source_data.smd_boundaries),
		ward_boundaries as (SELECT name, ST_SUBDIVIDE(geography::geometry) geography FROM source_data.ward_boundaries)
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
	--,row_number() over (partition by a.objectid) as crash_row_num
	,a.*
FROM analysis_data.dc_crashes_w_details a
--LEFT JOIN assessment_nabe2 b ON ST_Intersects(b.geography::geometry, a.geography::geometry)
LEFT JOIN anc_boundaries c ON ST_Intersects(c.geography::geometry, a.geography::geometry)
LEFT JOIN neighborhood_clusters d ON ST_Intersects(d.geography::geometry, a.geography::geometry)
LEFT JOIN smd_boundaries e ON ST_Intersects(e.geography::geometry, a.geography::geometry)
LEFT JOIN ward_boundaries f ON ST_Intersects(f.geography::geometry, a.geography::geometry)
) WITH DATA;
--SELECT 269790
--Query returned successfully in 5 min 26 secs.
delete from crashes_w_neighborhood where crash_row_num > 1;
--DELETE 27473

select * from source_data.cityworks_service_requests limit 100;
select * from crashes_w_neighborhood limit 100;
select * from analysis_data.dc_crashes_w_details limit 100;

select * from crashes_w_neighborhood where 

select ward_name, nbh_cluster_names, count(*)
from all311_w_neighborhood
where date_part('year', adddate)>=2015 
group by ward_name,nbh_cluster_names order by count(*) desc 

select * from all311_w_neighborhood
where ward_name = 'Ward 8';

select * from inner join source_data.roadway_blocks b on ST_DWithin(ST_Force2D(b.geometry::geometry)::geography,a.geography,70)

select * from source_data.intersection_points
where fullintersection in ('NEW YORK AVENUE NE AND FLORIDA AVENUE NE','FLORIDA AVENUE NE AND NEW YORK AVENUE NE')

create table tmp.anc_boundaries as (SELECT anc_id, ST_SUBDIVIDE(geography::geometry) geography FROM source_data.anc_boundaries);
CREATE INDEX geom_idx ON tmp.anc_boundaries USING GIST (geography);
create table tmp.neighborhood_clusters as (SELECT name, nbh_names, ST_SUBDIVIDE(geography::geometry) geography FROM source_data.neighborhood_clusters);
CREATE INDEX nbh_geom_idx ON tmp.neighborhood_clusters USING GIST (geography);
create table tmp.smd_boundaries as (SELECT smd_id, ST_SUBDIVIDE(geography::geometry) geography FROM source_data.smd_boundaries);
CREATE INDEX smd_geom_idx ON tmp.smd_boundaries USING GIST (geography);
create table tmp.ward_boundaries as (SELECT name, ST_SUBDIVIDE(geography::geometry) geography FROM source_data.ward_boundaries);
CREATE INDEX ward_geom_idx ON tmp.ward_boundaries USING GIST (geography);

CREATE INDEX crashes_geom_idx
  ON tmp.crashes_join
  USING GIST (geography);

DROP TABLE IF EXISTS tmp.crashes_nbh_ward;
CREATE TABLE tmp.crashes_nbh_ward 
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
    ,ROW_NUMBER() OVER (PARTITION BY a.objectid) as crash_row_num
	,a.*
FROM tmp.crashes_join a
LEFT JOIN tmp.anc_boundaries c ON ST_Intersects(c.geography::geometry, a.geography::geometry)
LEFT JOIN tmp.neighborhood_clusters d ON ST_Intersects(d.geography::geometry, a.geography::geometry)
LEFT JOIN tmp.smd_boundaries e ON ST_Intersects(e.geography::geometry, a.geography::geometry)
LEFT JOIN tmp.ward_boundaries f ON ST_Intersects(f.geography::geometry, a.geography::geometry)
) 
--finally returned in 6 minutes after putting damn indexes on everything...

select count(*) from analysis_data.all311
--6155
select count(*) from source_data.all311
select * from source_data.intersection_points limit 100;
drop table if exists all311_intersections;
create temp table  all311_intersections ON COMMIT PRESERVE ROWS AS (
select distinct a.*
	--, --b.block_name --
	, c.fullintersection
	, row_number() over (partition by a.objectid) as row_num
	from analysis_data.all311  a
--left join source_data.roadway_blocks b on ST_DWithin(ST_Force2D(b.geometry::geometry)::geography,a.geography,10)
left join source_data.intersection_points c on ST_DWithin(c.geography,a.geography,10)
) WITH DATA;
--10045

select * from all311_intersections order by objectid, row_num

select fullintersection, count(distinct objectid) as num_requests
from all311_intersections
group by fullintersection
order by 2 desc 

--Add ward, neighborhood cluster, ANC, SMD, and assessment neighborhood to TSA requests
DROP TABLE IF EXISTS all311_w_neighborhood;
CREATE TEMP TABLE all311_w_neighborhood  ON COMMIT PRESERVE ROWS AS (
	WITH --assessment_nabe2 as (SELECT assessment_nabe, ST_SUBDIVIDE(geography::geometry) geography FROM assessment_nabe),
		anc_boundaries as (SELECT anc_id, ST_SUBDIVIDE(geography::geometry) geography FROM source_data.anc_boundaries),
		neighborhood_clusters as (SELECT name, nbh_names, ST_SUBDIVIDE(geography::geometry) geography FROM source_data.neighborhood_clusters),
		smd_boundaries as (SELECT smd_id, ST_SUBDIVIDE(geography::geometry) geography FROM source_data.smd_boundaries),
		ward_boundaries as (SELECT name, ST_SUBDIVIDE(geography::geometry) geography FROM source_data.ward_boundaries)
SELECT 
-- 	b.assessment_nabe
-- 	,b.geography as assessment_nabe_boundary
	c.anc_id
	,c.geography as anc_boundary
	,d.name as nbh_cluster
	,d.nbh_names as nbh_cluster_names
	,d.geography as nbh_cluster_boundary
	,e.smd_id
	,e.geography as smd_boundary
	,f.name as ward_name 
	,f.geography as ward_boundary
	,row_number() over (partition by a.objectid) as tsa_row_num
	,a.*
FROM source_data.all311 a
--LEFT JOIN assessment_nabe2 b ON ST_Intersects(b.geography::geometry, a.geography::geometry)
LEFT JOIN anc_boundaries c ON ST_Intersects(c.geography::geometry, a.geography::geometry)
LEFT JOIN neighborhood_clusters d ON ST_Intersects(d.geography::geometry, a.geography::geometry)
LEFT JOIN smd_boundaries e ON ST_Intersects(e.geography::geometry, a.geography::geometry)
LEFT JOIN ward_boundaries f ON ST_Intersects(f.geography::geometry, a.geography::geometry)
) WITH DATA;
--SELECT 7071
--"This is not Traffic Calming.  Sent information on contacting DPW (see attached e-mail).  Closing."
delete from all311_w_neighborhood where tsa_row_num > 1;
--DELETE 916

select * from crashes_w_neighborhood where  date_part('year', fromdate)>=2015 and anc_id = '5B' and (total_pedestrians>0 or total_bicyclists>0)
--2608 all
--144 biker or pedestrian
select * from crashes_w_neighborhood where  date_part('year', fromdate)>=2015 and anc_id = '5B' and (peds_under_12>0 or bikers_under_18>0)
--10 kids

select * from all311_w_neighborhood where  date_part('year', adddate)>=2015 and anc_id = '5B';
--210

select count(*) from crashes_w_neighborhood where date_part('year', fromdate)>=2015 and peds_under_12>0 and total_injuries>0
--407

select ST_Force2D(geometry::geometry)::geography, * from source_data.roadway_blocks 
where 
	bikelane_contraflow is not null 
	or bikelane_conventional is not null
	or bikelane_protected is not null
	or bikelane_buffered is not null
--1143 blocks 

select ST_Force2D(geometry::geometry)::geography, * from source_data.roadway_blocks 
where bikelane_protected is not null or bikelane_buffered is not null


select * from crashes_w_neighborhood where date_part('year', fromdate)>=2015 and bikers_under_18=0 and total_bicyclists>0
--273 under 18
--3379 adults 
and ward_name in ('Ward 3', 'Ward 8')
--only 226 total in those 2 wards 
select ward_name, count(*) from crashes_w_neighborhood where date_part('year', fromdate)>=2015 and bikers_under_18=0 and total_bicyclists>0
group by ward_name order by count(*) desc;
/*
"Ward 2"	1167
"Ward 6"	682
"Ward 1"	619
"Ward 5"	363
"Ward 4"	220
"Ward 3"	147
"Ward 7"	115
"Ward 8"	79
	5
*/
select ward_name, count(*) from crashes_w_neighborhood where date_part('year', fromdate)>=2015 and bikers_under_18>0
group by ward_name order by count(*) desc;
/*
"Ward 6"	49
"Ward 1"	42
"Ward 2"	41
"Ward 5"	36
"Ward 4"	32
"Ward 7"	29
"Ward 3"	22
"Ward 8"	22
*/
select ward_name, count(*) from crashes_w_neighborhood where date_part('year', fromdate)>=2015 and total_pedestrians>0
group by ward_name order by count(*) desc;
/*
"Ward 2"	1452
"Ward 6"	995
"Ward 5"	815
"Ward 8"	786
"Ward 1"	692
"Ward 7"	637
"Ward 4"	511
"Ward 3"	377
	27
	*/
select ward_name, count(*) from crashes_w_neighborhood where date_part('year', fromdate)>=2015 and peds_over_80>0
group by ward_name order by count(*) desc;
/*
over 70
"Ward 2"	50
"Ward 3"	43
"Ward 6"	38
"Ward 5"	34
"Ward 1"	23
"Ward 4"	20
"Ward 8"	17
"Ward 7"	13

over 80
"Ward 2"	15
"Ward 6"	9
"Ward 3"	9
"Ward 5"	8
"Ward 8"	6
"Ward 1"	6
"Ward 4"	4
"Ward 7"	1
*/
select ward_name, count(*) from crashes_w_neighborhood where date_part('year', fromdate)>=2015 and peds_under_12>0
group by ward_name order by count(*) desc;
/*
"Ward 8"	121
"Ward 7"	75
"Ward 4"	68
"Ward 5"	67
"Ward 6"	60
"Ward 1"	38
"Ward 2"	25
"Ward 3"	23
	1
*/
select * from crashes_w_neighborhood where date_part('year', fromdate)>=2015 and peds_under_12>0;
--478
select count(*) from crashes_w_neighborhood where date_part('year', fromdate)>=2015 and total_pedestrians>0 
--6292
select count(*) from crashes_w_neighborhood where date_part('year', fromdate)>=2015 and total_pedestrians>0 
--6292

select nbh_cluster_names, count(*) as num_crashes
from crashes_w_neighborhood where date_part('year', fromdate)>=2015 and peds_under_12>0
group by nbh_cluster_names order by count(*) desc 

select distinct a.*, b.block_name from all311_w_neighborhood a
inner join source_data.roadway_blocks b on ST_DWithin(ST_Force2D(b.geometry::geometry)::geography,a.geography,10)
where nbh_cluster_names in ('Capitol Hill, Lincoln Park','Union Station, Stanton Park, Kingman Park')
limit 100;

select grades, count(*)
from source_data.charter_schools
group by grades order by count(*) desc

select grades, count(*)
from source_data.public_schools
group by grades order by count(*) desc

select * from source_data.public_schools limit 100;

SELECT 
	0 as charter_school
	,1 as public_school
	,grades
	,name as school_name 
	,case when grades like 'PK3%' or grades like 'PK4%' or grades like '1st%' or grades like '4th%' or grades like '%KG%' then 1 else 0 end as ES 
	,case when grades like '6th%' or grades like '%8th%' then 1 else 0 end as MS 
	,case when grades like '9th%' or grades like '%12th%' or grades ='Adult' or grades ='Alternative' then 1 else 0 end as HS
	,geography
from source_data.public_schools
UNION ALL
SELECT 
	1 as charter_school
	,0 as public_school
	,grades
	,name as school_name 
	,case when grades like 'PK3%' or grades like 'PK4%' or grades like '1st%' or grades like '4th%' or grades like '%KG%' then 1 else 0 end as ES 
	,case when grades like '6th%' or grades like '%8th%' then 1 else 0 end as MS 
	,case when grades like '9th%' or grades like '%12th%' or grades ='Adult' or grades ='Alternative' then 1 else 0 end as HS
	,geography
from source_data.charter_schools

DROP TABLE IF EXISTS tmp_schools;
CREATE TEMP TABLE tmp_schools ON COMMIT PRESERVE ROWS 
AS ( 
    SELECT 
        0 as charter_school
        ,1 as public_school
        ,grades
        ,name as school_name 
        ,case when grades like 'PK3%' or grades like 'PK4%' or grades like '1st%' or grades like '4th%' or grades like '%KG%' then 1 else 0 end as ES 
        ,case when grades like '6th%' or grades like '%8th%' then 1 else 0 end as MS 
        ,case when grades like '9th%' or grades like '%12th%' or grades ='Adult' or grades ='Alternative' then 1 else 0 end as HS
        ,geography
    from source_data.public_schools
    UNION ALL
    SELECT 
        1 as charter_school
        ,0 as public_school
        ,grades
        ,name as school_name 
        ,case when grades like 'PK3%' or grades like 'PK4%' or grades like '1st%' or grades like '4th%' or grades like '%KG%' then 1 else 0 end as ES 
        ,case when grades like '6th%' or grades like '%8th%' then 1 else 0 end as MS 
        ,case when grades like '9th%' or grades like '%12th%' or grades ='Adult' or grades ='Alternative' then 1 else 0 end as HS
        ,geography
    from source_data.charter_schools
) WITH DATA;

select  array_agg(distinct b.school_name) as near_schools, a.* from analysis_data.dc_crashes_w_details a
inner join analysis_data.all_schools b on ST_DWithin(b.geography,a.geography,200)
--inner join source_data.public_schools b on ST_DWithin(ST_Force2D(b.geography::geometry)::geography,a.geography,200)
where total_pedestrians >0
--and b.name in ('School-Within-School', 'Ludlow-Taylor Elementary School','J.O. Wilson Elementary School','Stuart-Hobson Middle School')
--1862 rows affected w/out array
--1730 rows affected w/array
group by (
a.anc_id
,a.anc_boundary
,a.nbh_cluster
,a.nbh_cluster_names
,a.nbh_cluster_boundary
,a.smd_id
,a.smd_boundary
,a.ward_name
,a.ward_boundary
,a.objectid
,crimeid
,reportdate
,fromdate
,todate
,a.address
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
,peds_over_80
,peds_under_12
,bikers_over_70
,bikers_under_18
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
,locationerror
,lastupdatedate
,blockkey
,subblockkey
,a.geography)

select * from analysis_data.dc_crashes_w_details limit 100;

select * from information_schema.columns where table_name = 'dc_crashes_w_details'

select distinct a.*, b.name from all311_w_neighborhood a
left join source_data.public_schools b on ST_DWithin(ST_Force2D(b.geography::geometry)::geography,a.geography,200)
where nbh_cluster_names in ('Capitol Hill, Lincoln Park','Union Station, Stanton Park, Kingman Park')

select distinct a.* from all311_w_neighborhood a
where nbh_cluster_names in ('Capitol Hill, Lincoln Park','Union Station, Stanton Park, Kingman Park')

select * from analysis_data.all311 limit 100;

select * from analysis_data.all_schools where es = 0 and ms = 0 and hs = 0;

select ward_name, nbh_cluster_names, count(*) as num_tsa_requests
from all311_w_neighborhood 
group by ward_name, nbh_cluster_names order by count(*) desc 
--"Congress Heights, Bellevue, Washington Highlands"
--"Brightwood Park, Crestwood, Petworth"
--"Douglas, Shipley Terrace"

select * from crashes_w_neighborhood where date_part('year', fromdate)>=2015 and peds_under_12>0 
and nbh_cluster_names = 'Congress Heights, Bellevue, Washington Highlands'
--41 

select * from crashes_w_neighborhood where date_part('year', fromdate)>=2015 and total_pedestrians>0 
and nbh_cluster_names = 'Congress Heights, Bellevue, Washington Highlands'
--284

select * from all311_w_neighborhood where date_part('year', adddate)>=2015 
and nbh_cluster_names = 'Congress Heights, Bellevue, Washington Highlands'
--203

select * from crashes_w_neighborhood where date_part('year', fromdate)>=2015 and peds_under_12>0 
and nbh_cluster_names like '%Chevy Chase%'
--8

select * from crashes_w_neighborhood where date_part('year', fromdate)>=2015 and total_pedestrians>0 
and nbh_cluster_names like '%Chevy Chase%'
--49

select * from all311_w_neighborhood where date_part('year', adddate)>=2015 
and nbh_cluster_names like '%Chevy Chase%'
--100

select * from crashes_w_neighborhood where date_part('year', fromdate)>=2015 and peds_under_12>0 
and nbh_cluster_names like '%Friendship Heights%'
--5

select * from crashes_w_neighborhood where date_part('year', fromdate)>=2015 and total_pedestrians>0 
and nbh_cluster_names like '%Friendship Heights%'
--115

select * from all311_w_neighborhood where date_part('year', adddate)>=2015 
and nbh_cluster_names like '%Friendship Heights%'
--116

/*
data exploration
select * from all311_w_neighborhood
select * from source_data.ward_boundaries
select * from source_data.neighborhood_clusters

select * from source_data.anc_boundaries

select * from source_data.smd_boundaries

select * from source_data.pulsepoint_stream where cast(call_received_datetime as date) = '2021-01-04'
order by incident_id, scrape_datetime

select * from analysis_data.dc_crashes_w_details where date_part('year', fromdate) = 2020 and peds_under_12 >0;
*/
