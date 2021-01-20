select * from source_data.intersection_points where objectid = '5286'

select a.*, ST_Distance(a.geography, (select geography from source_data.intersection_points where objectid = '5286')) from analysis_data.dc_crashes_w_details a 
where ST_DWIthin(a.geography, (select geography from source_data.intersection_points where objectid = '5286'),10)
and date_part('year',fromdate) >= 2015
--4 accidents since 2015 with a police report

select a.* from analysis_data.all311 a 
where ST_DWIthin(a.geography, (select geography from source_data.intersection_points where objectid = '5286'),30)
--4 requests that are specifically TSA's 

select * from source_data.pulsepoint

select max(scrape_datetime) from source_data.pulsepoint
select max(fromdate) from analysis_data.dc_crashes_w_details

select a.* from source_data.cityworks_service_requests a 
where ST_DWIthin(a.geography, (select geography from source_data.intersection_points where objectid = '5286'),30)
--the other 18 are also nearly all safety related

select a.* from source_data.cityworks_work_orders a 
where ST_DWIthin(a.geography, (select geography from source_data.intersection_points where objectid = '5286'),30)
--the other 18 are also nearly all safety related

select ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography, a.* from source_data.moving_violations a
where ST_DWIthin(ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography, (select geography from source_data.intersection_points where objectid = '5286'),100)
--accident indicators: 2019-09-07, 2018-09-28
--only 5 total since 2018
--zero in 2020

select a.* from source_data.moving_violations a where location like '%MILITARY%' and location like '%4%'