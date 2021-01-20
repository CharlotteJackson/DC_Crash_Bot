drop table if exists unique_intersections;
create temp table  unique_intersections ON COMMIT PRESERVE ROWS AS (
select distinct a.*
	, row_number() over (partition by a.intersectionid) as row_num
	from source_data.intersection_points a
) WITH DATA;

select * from source_data.intersection_points limit 100;
select * from source_data.intersection_points where st1name = 'EAST CAPITOL' and st2name = '5TH'
---intersection id = 16130 

select a.* from source_data.cityworks_service_requests a
where ST_DWithin((select geography from source_data.intersection_points where objectid = '5705'),a.geography, 10) 

select a.* from source_data.all311 a
where ST_DWithin((select geography from source_data.intersection_points where objectid = '5705'),a.geography, 10) 
order by adddate 
--8 requests relate to road/traffic safety

select a.* from source_data.cityworks_work_orders a
where ST_DWithin((select geography from source_data.intersection_points where objectid = '5705'),a.geography, 10) 

select a.* from source_data.pulsepoint a
where ST_DWithin((select geography from source_data.intersection_points where objectid = '5705'),a.geography, 40) 

select a.* from analysis_data.dc_crashes_w_details a
where ST_DWithin((select geography from source_data.intersection_points where objectid = '5705'),a.geography, 30) 
--11 crashes with a police report 

select a.* from source_data.vision_zero a
where ST_DWithin((select geography from source_data.intersection_points where objectid = '5705'),a.geography, 10) 
--at least one vision zero request (though i dont trust this data bc it basically disappears after 2015)

select ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography, a.* from source_data.moving_violations a 
where ST_DWIthin(ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography, (select geography from source_data.intersection_points where objectid = '5705'),100)
--9 moving violations within 100 yards, of which 4 actually related to traffic safety (vs registration or insurance)

select a.* from analysis_data.all311 a
where ST_DWithin((select geography from source_data.intersection_points where objectid = '5705'),a.geography, 10) 

select a.* from source_data.cityworks_work_orders a where sourceworkorderid is not null limit 100;


select 
	servicecode
	,servicecodedescription
	,servicetypecodedescription
	,count(*)
from source_data.all311
group by 
servicecode
	,servicecodedescription
	,servicetypecodedescription
order by 
servicecode
	,servicecodedescription
	,servicetypecodedescription



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

select date_part('year', adddate) as year, count(*)
from source_data.all311
group by date_part('year', adddate) order by date_part('year', adddate)



select * from  analysis_data.all311 limit 100;

select * from tmp.twitter