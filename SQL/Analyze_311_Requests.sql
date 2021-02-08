--find service code descriptions that are confirmed to be related to traffic safety
--confirmed ones: MARKMAIN, SIGNMISS, S0286
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
	
--check the details 
select * from source_data.all311
where servicecode in ('MARKMAIN', 'SIGNMISS', 'S0286', 'CONFSIGN', 'DDOTCITA'
					 ,'DPWCORTR', 'EMERTRAN', 'MARKMODI', 'MARKINST', 'MARKREMO', 'S0021', 'S0046','S0287', 'S0376', 'S0406'
					 ,'SAROTOSC', 'SCCRGUPR','SIGTRAMA', 'SPSTDAMA')
and details is not null
order by servicecode, adddate
--confsign seems to be mostly parking related

select * from analysis_data.all311 where ST_DWithin(geography, (select geography from source_data.intersection_points where objectid = '3956'),100)
order by adddate 

select * from source_data.all311 --final list
where servicecode in ('MARKMAIN', 'MARKMODI', 'MARKINST',   'S0376'
					 ,'SAROTOSC', 'SCCRGUPR', 'SPSTDAMA')
and details is not null
order by servicecode, adddate
--dddot citation - no
--DPWCORTR - no
--emertran - no records with comments
--markinst and markmodi is DEFINITELY safety related 
--markmain questionable
--S0021 bike issues seem to be a lot of abandoned bikes, occasional "pls add bike lane" - maybe "b"
--S0046 bus/rail issues seem to be lots of bus stop maintenance stuff 
--S0286 Sign Replacement seems to be mostly signs that were knocked down by drivers and need to be replaced
--S0287 Sign Removal Investigation is mostly temporary signs that are no longer relevant but were not removed
--S0376 sign new investigation is DEFINITELY safety related - eg "very dangerous intersection, need traffic light or stop sign"
--also "4 way Stop signs need to be placed at 34th street and Croffut to avoid accidents which happen frequently by cars parking in the intersection (one happened again today).  Also a danger to pedestrians crossing Croffut at 34th street.""
--S0406 street repair is questionable. eg see 1685090 "I would like someone to connect with me regarding the area by safeway and the police station on the corner. It is alarming to know that is a very unsafe area to walk and bike and it is near a government building helping to protect us."
--also some speed humps requested
--but also lots of potholes, repaving etc
--SIGNMISS is lots of existing signs that were knocked down by drivers
--SIGTRAMA is existing traffic lights that stopped working
--MARKREMO is definitely sometimes safety related (see 1949811) but not enough examples to make definitive call
--'SAROTOSC', 'SCCRGUPR', 'SPSTDAMA' all unambiguously safety related
--'DDOTCITA','DPWCORTR', 'EMERTRAN',
select * from source_data.all311
where servicecode = 'SPSTDAMA'
and details is not null
order by servicecode, adddate

select * from source_data.all311
where servicecode = 'SPSTDAMA' and date_part('year', adddate) = 2021

select servicecode
	, count(*)
from source_data.all311
where servicecode in ('MARKMAIN', 'MARKMODI', 'MARKINST',   'S0376','SAROTOSC', 'SCCRGUPR', 'SPSTDAMA')
group by servicecode order by count(*) desc 
--this more than doubles the number of safety requests

select * from source_data.intersection_points where st1name ilike 'MINN%' and st2name ilike 'BENN%'

select * from source_data.crash_details where crimeid = '26631724'
select * from source_data.crash_details where persontype ilike '%Driver%' and licenseplatestate not ilike '%None%'
select * from analysis_data.dc_crashes_w_details where ST_DWithin(geography, (select geography from source_data.intersection_points where objectid = '12142'),100)
and (total_pedestrians > 0 or total_bicyclists>0)

select * from source_data.pulsepoint where ST_DWithin(geography, (select geography from source_data.intersection_points where objectid = '12142'),100)
and (total_pedestrians > 0 or total_bicyclists>0)

select * from analysis_data.all311 where ST_DWithin(geography, (select geography from source_data.intersection_points where objectid = '4159'),100)
order by adddate 

select servicecode
	, sum(case when date_part('year', adddate) = 2015 then 1 else 0 end) as num_2015
	, sum(case when date_part('year', adddate) = 2016 then 1 else 0 end) as num_2016
	, sum(case when date_part('year', adddate) = 2017 then 1 else 0 end) as num_2017
	, sum(case when date_part('year', adddate) = 2018 then 1 else 0 end) as num_2018
	, sum(case when date_part('year', adddate) = 2019 then 1 else 0 end) as num_2019
	, sum(case when date_part('year', adddate) = 2020 then 1 else 0 end) as num_2020
	, sum(case when date_part('year', adddate) = 2021 then 1 else 0 end) as num_2021
	,count(*) as total
from source_data.all311
where servicecode in ('MARKMAIN', 'MARKMODI', 'MARKINST',   'S0376','SAROTOSC', 'SCCRGUPR', 'SPSTDAMA')
group by servicecode order by count(*) desc

select case when details is null then 'no details' else 'details' end as status
	, sum(case when date_part('year', adddate) = 2015 then 1 else 0 end) as num_2015
	, sum(case when date_part('year', adddate) = 2016 then 1 else 0 end) as num_2016
	, sum(case when date_part('year', adddate) = 2017 then 1 else 0 end) as num_2017
	, sum(case when date_part('year', adddate) = 2018 then 1 else 0 end) as num_2018
	, sum(case when date_part('year', adddate) = 2019 then 1 else 0 end) as num_2019
	, sum(case when date_part('year', adddate) = 2020 then 1 else 0 end) as num_2020
	, sum(case when date_part('year', adddate) = 2021 then 1 else 0 end) as num_2021
	,count(*) as total
from source_data.all311
--where servicecode in ('MARKMAIN', 'MARKMODI', 'MARKINST',   'S0376','SAROTOSC', 'SCCRGUPR', 'SPSTDAMA')
group by case when details is null then 'no details' else 'details' end order by count(*) desc


select * from source_data.cityworks_service_requests limit 100;

select ward_name, nbh_cluster_names, count(*)
from analysis_data.all311
group by ward_name, nbh_cluster_names
order by count(*) desc 

select * from analysis_data.all311 where details ilike '%enforce%' or details ilike '%mpd%' or details like '%police%'

select * from analysis_data.roadway_blocks where routename ilike '%17TH ST NE%'

select * from analysis_data.all311 where ward_name = 'Ward 8'
order by adddate

select * from analysis_data.all311 where nbh_cluster_names = 'Historic Anacostia'
order by adddate
