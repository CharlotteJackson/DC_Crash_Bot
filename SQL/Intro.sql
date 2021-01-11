/********************************
Intro the Plsepoint data
*******************************/

select * from source_data.pulsepoint_stream where cast(call_received_datetime as date) >='2020-12-30'
order by incident_id, scrape_datetime;

select * from source_data.pulsepoint;

--limit crashes to just the dates in the pulsepoint data
drop table if exists tmp_crashes;
create temp table tmp_crashes on commit preserve rows as (
select distinct a.* 
	from analysis_data.dc_crashes_w_details a
	where cast(fromdate as date) between '2020-12-22' and '2020-12-31'
	) with data;

--join crashes and pulsepoint
DROP TABLE IF EXISTS temp_join;
CREATE TEMP TABLE temp_join on commit preserve rows as (
SELECT distinct
	pp.incident_id
	,pp.call_received_datetime
	,pp.call_closed_datetime
	,pp.fulldisplayaddress
	,pp.incident_type
	,pp.unit_status_transport
	,pp.transport_unit_is_amr
	,pp.transport_unit_is_non_amr
	,pp.num_units_responding
	,pp.geography as pp_geography
	,ST_Distance(pp.geography,crash.geography) AS distance_between_events
	,crash.*
FROM (select * from source_data.pulsepoint where cast(CALL_RECEIVED_DATETIME as date) between '2020-12-22' and '2020-12-31') pp 
FULL OUTER JOIN tmp_crashes crash ON ST_DWITHIN(ST_Force2D(crash.geography::geometry), pp.geography, 150)--reported locations within 100 meters of each other?
	AND cast(fromdate as date) =cast(CALL_RECEIVED_DATETIME as date)--fromdate doesn't have a timestamp, so same date is the most granular possible join
	AND reportdate >= call_received_datetime --but dont want to match records that were reported on prior to the call coming in
) WITH DATA;

--select * from temp_join;
--select count(distinct incident_id) from temp_join;
--210
--select count(distinct incident_id) from temp_join where objectid is not null;
--102 

--Generate neighborhood boundaries from the address dataset	
CREATE TEMP TABLE nabe_boundaries ON COMMIT PRESERVE ROWS AS (
SELECT 
    d.assessment_nbhd AS nabe,
	ST_ConcaveHull(ST_Collect(d.geography::geometry), 0.99) AS geography
FROM source_data.address_points AS d
GROUP BY d.assessment_nbhd
)
WITH DATA;

--select * from nabe_boundaries;
	
--Add the neighborhood to each crash record
DROP TABLE IF EXISTS crashes_w_neighborhood;
CREATE TEMP TABLE crashes_w_neighborhood  ON COMMIT PRESERVE ROWS AS (
SELECT b.nabe as Crash_Nabe, c.nabe as PP_Nabe, coalesce(b.nabe, c.nabe) as final_nabe, a.*
FROM temp_join a
LEFT JOIN nabe_boundaries b ON ST_Intersects(b.geography::geometry, a.geography::geometry)
LEFT JOIN nabe_boundaries c ON ST_Intersects(c.geography::geometry, a.pp_geography::geometry)
) WITH DATA;

--select * from crashes_w_neighborhood where final_nabe is null

--Add pp injuries info
DROP TABLE IF EXISTS crashes_injuries;
CREATE TEMP TABLE crashes_injuries  ON COMMIT PRESERVE ROWS AS (
SELECT *
	,unit_status_transport as Total_Injuries_PP
	,case when total_injuries is null or total_injuries < unit_status_transport then unit_status_transport-total_injuries end as Extra_Injuries_PP
	,case when objectid is null then incident_id else null end as Extra_Crash_PP
FROM crashes_w_neighborhood
) WITH DATA;

select 
	final_nabe as Neighborhood
	,count(distinct objectid) as MPD_Reported_Crashes
	,count(distinct Extra_Crash_PP) as DCFEMS_Crashes_Not_In_MPD_Reports
	,sum(total_injuries) as MPD_Reported_Injuries
	,sum(Extra_Injuries_PP) as DCFEMS_Injuries_Not_In_MPD_Reports
from crashes_injuries
group by final_nabe order by count(distinct objectid) desc