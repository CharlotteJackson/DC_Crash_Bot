SET TIME ZONE 'America/New_York';

drop table if exists viz.february_2021_crashes;
create table viz.february_2021_crashes as 
select 
objectid
,fromdate
,geography
,latitude::numeric
,longitude::numeric
,address
,ward_name
,nbh_cluster_names
,comp_plan_area
,case when total_bicyclists + total_pedestrians = 0 then 'Cars Only'
	when total_pedestrians>0 then 'Car/Pedestrian'
	when total_bicyclists>0 then 'Car/Bicycle'
	end as Crash_Type
,case when dcfunctionalclass_desc ilike '%interstate%' or int_road_types::text ilike '%interstate%' 
	or int_road_types::text ilike '%freeway%' or dcfunctionalclass_desc ilike '%freeway%'
	then 1 else 0 end as Crash_On_Interstate
, NULL as Scanner_Audio_Missing
,case when pp_agency_incident_id is null then 'MPD Only' else 'MPD and DCFEMS' end as Crash_Category
,0 as national_park
from analysis_data.dc_crashes_w_details 
where date_part('year', fromdate) = 2021 and date_part('month', fromdate) = 2
union all 
select 
a.incident_id 
,call_received_datetime::date
,geography
,latitude::numeric
,longitude::numeric
,fulldisplayaddress
,ward_name
,nbh_cluster_names
,comp_plan_area
,case when b.pedestrian_involved + b.bike_involved = 0 then 'Cars Only'
	when pedestrian_involved=1 then 'Car/Pedestrian'
	when bike_involved=1 then 'Car/Bicycle'
	end as Crash_Type
,case when dcfunctionalclass_desc ilike '%interstate%' or int_road_types::text ilike '%interstate%' 
	or int_road_types::text ilike '%freeway%' or dcfunctionalclass_desc ilike '%freeway%'
	or b.Incident_On_Interstate = 1
	then 1 else 0 end as Crash_On_Interstate
,b.audio_missing as Scanner_Audio_Missing
,'DCFEMS Only' as Crash_Category
,national_park
from analysis_data.pulsepoint a
left join source_data.february_2021_dcfems_scanner_audio b on a.incident_id = b.incident_id
where a.agency_id = 'EMS1205'
and a.crash_objectid is null 
and date_part('year', call_received_datetime) = 2021
and date_part('month', call_received_datetime) = 2
and a.incident_type <> 'RES';
