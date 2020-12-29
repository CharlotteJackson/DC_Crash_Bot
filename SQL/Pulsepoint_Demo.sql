/*************************************************
Exploratory analysis of first Pulsepoint data dump
Pulsepoint API documentation: https://docs.google.com/document/pub?id=1qMdahl1E9eE4Rox52bmTA2BliR1ve1rjTYAbhtMeinI#id.q4mai5x52vi6
Incident types of TC and TCE indicate traffic collisions
Unit transport status of YES indicates someone was transported to the hospital (per my first responder brother)
*************************************************/

--step 1 look at pulsepoint data
select cast(CALL_RECEIVED_DATETIME as date),count(*) 
from source_data.pulsepoint 
group by cast(CALL_RECEIVED_DATETIME as date) order by cast(CALL_RECEIVED_DATETIME as date)
select max(fromdate) from analysis_data.dc_crashes_w_details;

--step 2 limit to just the dates in the pulsepoint data
drop table if exists tmp_crashes;
create temp table tmp_crashes on commit preserve rows as (
select distinct a.* 
	from analysis_data.dc_crashes_w_details a
	inner join source_data.pulsepoint b on cast(fromdate as date) =cast(CALL_RECEIVED_DATETIME as date)
	) with data;
	
select * from tmp_crashes;

--Step 3 join them together 
DROP TABLE IF EXISTS temp_join;
CREATE TEMP TABLE temp_join on commit preserve rows as (
SELECT distinct
	pp.incident_id
	,pp.call_received_datetime
	,pp.call_closed_datetime
	,pp.fulldisplayaddress
	,pp.incident_type
	,pp.unit_status_transport
	,pp.num_units_responding
	,pp.geography as pp_geography
	,crash.*
FROM source_data.pulsepoint pp 
LEFT OUTER JOIN tmp_crashes crash ON ST_DWITHIN(ST_Force2D(crash.geography::geometry), pp.geography, 20)--reported locations within 10 meters of each other?
	AND cast(fromdate as date) =cast(CALL_RECEIVED_DATETIME as date)--fromdate doesn't have a timestamp, so same date is the most granular possible join
WHERE cast(CALL_RECEIVED_DATETIME as date) <= '2020-12-27'		
) WITH DATA;

SELECT * FROM temp_join
where unit_status_transport = 'YES' and objectid is null;

SELECT * FROM temp_join
where unit_status_transport = 'YES' and objectid is not null;

--How many PP crash records did not show up in crashes data?
select * from temp_join
where crimeid is null;
--2 records out of 9
--One is on Eastern Ave and was maybe counted in MD crash data instead

--How many Open Data DC crash records don't have a corresponding PP call?
select * from temp_join
where incident_id is null;
--46 records out of 55

--How many PP records joined to multiple crash records?
select * from temp_join 
where incident_id in (select incident_id from temp_join group by incident_id having count(*)>1)
order by incident_id
--2/9 each joined to 4 crash records
--IT looks like these two incidents are themselves referring to the same crash - same address and nearly the same call time
--however, not clear which of the DC open data crash records are matches 

--How many crash records joined to multiple PP records?
select * from temp_join 
where crimeid in (select crimeid from temp_join group by crimeid having count(*)>1)
order by crimeid;
--same 8 records as above