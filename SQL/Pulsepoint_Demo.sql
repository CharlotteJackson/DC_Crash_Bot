/*************************************************
Exploratory analysis of first Pulsepoint data dump
Pulsepoint API documentation: https://docs.google.com/document/pub?id=1qMdahl1E9eE4Rox52bmTA2BliR1ve1rjTYAbhtMeinI#id.q4mai5x52vi6
Incident types of TC and TCE indicate traffic collisions
Unit transport status of YES indicates someone was transported to the hospital (per my first responder brother)
*************************************************/

--Step 1 Create a temporary table of just traffic collisions (excluding medical emergencies and other incident types)
--Calculate a field for how many units responded to a call
DROP TABLE IF EXISTS pp_traffic_calls;
CREATE TEMP TABLE pp_traffic_calls on commit preserve rows as (
select STATUS_AT_LOAD
	,incident_id
	,CALL_RECEIVED_DATETIME
	,FULLDISPLAYADDRESS
	,INCIDENT_TYPE
	,UNIT_STATUS_TRANSPORT
	,GEOMETRY AS PP_GEOMETRY
	,UNIT_JSON
	, JSONB_ARRAY_LENGTH(unit_json::jsonb) as Num_Units_Responding
from source_data.pulsepoint
WHERE incident_type in ('TC', 'TCE')
	) WITH DATA;

select * from pp_traffic_calls;
--9 active and recent traffic collisions 
--2020-11-18 only date in dataset

--Step 2 create temp table of crashes limited to just the date in the Pulsepoint data 
DROP TABLE IF EXISTS crash_subset;
CREATE TEMP TABLE crash_subset on commit preserve rows as (
SELECT * 
	FROM analysis_data.dc_crashes_w_details WHERE CAST(fromdate as date )='2020-11-18'
	) WITH DATA;
--55 rows 

--Step 3 join them together 
DROP TABLE IF EXISTS temp_join;
CREATE TEMP TABLE temp_join on commit preserve rows as (
SELECT DATE_PART('hour', reportdate::time) as crash_hr, DATE_PART('hour',CALL_RECEIVED_DATETIME::time ) as pp_hr
	--"report date" has a timestamp while "from date" does not, but my guess is "from date" is when the crash actually happened
	--and "report date" is when MPD learned about it
	, *
FROM pp_traffic_calls pp 
FULL OUTER JOIN crash_subset crash ON ST_DWITHIN(crash.geometry, pp.PP_GEOMETRY, 0.003)--is this the correct boundary? i think it might be too wide
	AND cast(fromdate as date) =cast(CALL_RECEIVED_DATETIME as date)
		) WITH DATA;

SELECT * FROM temp_join;

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