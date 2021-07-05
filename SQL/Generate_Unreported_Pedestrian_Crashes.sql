------------------------------
--set correct time zone
------------------------------
SET TIME ZONE 'America/New_York';
select * from source_Data.ward_boundaries
update source_Data.ward_boundaries set geometry = geometry::geography
alter table source_Data.ward_boundaries alter column geography set data type geography USING geography::geography
CREATE INDEX IF NOT EXISTS ward_geom_idx ON source_Data.ward_boundaries USING GIST (geography::geography);
---------------------------------------------------------------------------
--Step 1: Prep Pulsepoint tables for joining
--Unnested IDs of responding units
--Unnested Quadrants
--Unnested and Processed Addresses
---------------------------------------------------------------------------
--Analysis set: Calls received after 2021-05-15 for DCFEMS (agency ID EMS1205)
drop table if exists tmp_pulsepoint;
create temporary table tmp_pulsepoint on commit preserve rows as (
select *
	from source_data.pulsepoint where call_received_datetime::date >= '2021-05-15' and agency_id = 'EMS1205'
) with data;

--Un-nest the unit numbers
drop table if exists unit_ids;
create temporary table unit_ids on commit preserve rows as (
select incident_id, call_received_datetime::date as call_date, unnest(unit_ids) as unit_id
	from tmp_pulsepoint
) with data;

--Un-nest the quadrants into both spaced and un-spaced versions
drop table if exists quadrants;
create temporary table quadrants on commit preserve rows as (
	select incident_id
	, call_received_datetime::date as call_date
	,case 
		when replace(fulldisplayaddress,',',' ') like '% SW %' then 'south west'
		when replace(fulldisplayaddress,',',' ') like '% NW %' then 'north west'
		when replace(fulldisplayaddress,',',' ') like '% NE %' then 'north east'
		when replace(fulldisplayaddress,',',' ') like '% SE %' then 'south east'
	end as quadrant
	from tmp_pulsepoint
	UNION ALL
	select incident_id
	, call_received_datetime::date
	,case 
		when replace(fulldisplayaddress,',',' ') like '% SW %' then 'southwest'
		when replace(fulldisplayaddress,',',' ') like '% NW %' then 'northwest'
		when replace(fulldisplayaddress,',',' ') like '% NE %' then 'northeast'
		when replace(fulldisplayaddress,',',' ') like '% SE %' then 'southeast'
	end as quadrant
	from tmp_pulsepoint
) with data;

--Un-nest the addresses
--Split the intersections
--Remove the street numbers, quadrants, and city name
--Add variations on MLK Jr Ave and Capitol St
drop table if exists addresses;
create temporary table addresses on commit preserve rows as (
select incident_id, call_received_datetime::date as call_date, fulldisplayaddress, fulldisplayaddress as original_address
	from tmp_pulsepoint
	--exclude interstates
	where fulldisplayaddress not like '%I695%'
	and fulldisplayaddress not like '%I295%'
	and fulldisplayaddress not like '%I395%'
	and fulldisplayaddress not like '%I66%'
) with data;

update addresses set fulldisplayaddress = replace(fulldisplayaddress, ', WASHINGTON, DC',' ');

--then replace dashes and commas with spaces
update addresses set fulldisplayaddress = replace(fulldisplayaddress, ',',' ');
update addresses set fulldisplayaddress = replace(fulldisplayaddress, '-',' ');

--remove numbers that are followed by a space (eg, numbers that aren't street names)
update addresses set fulldisplayaddress = regexp_replace(regexp_replace(fulldisplayaddress, ' STE [A-Z]{0,1}[0-9]{1,3}',''), '([0-9]{1,4} )','','g');

--remove quadrants
update addresses set fulldisplayaddress = replace(fulldisplayaddress, ' NW ',' ');
update addresses set fulldisplayaddress = replace(fulldisplayaddress, ' NE ',' ');
update addresses set fulldisplayaddress = replace(fulldisplayaddress, ' SW ',' ');
update addresses set fulldisplayaddress = replace(fulldisplayaddress, ' SE ',' ');

--replace street/avenue/road/drive/square/place/lane/plaza/terrace abbreviations
update addresses set fulldisplayaddress = replace(fulldisplayaddress, ' ST ',' STREET ');
update addresses set fulldisplayaddress = replace(fulldisplayaddress, ' AVE ',' AVENUE ');
update addresses set fulldisplayaddress = replace(fulldisplayaddress, ' RD ',' ROAD ');
update addresses set fulldisplayaddress = replace(fulldisplayaddress, ' DR ',' DRIVE ');
update addresses set fulldisplayaddress = replace(fulldisplayaddress, ' SQ ',' SQUARE ');
update addresses set fulldisplayaddress = replace(fulldisplayaddress, ' PL ',' PLACE ');
update addresses set fulldisplayaddress = replace(fulldisplayaddress, ' LN ',' LANE ');
update addresses set fulldisplayaddress = replace(fulldisplayaddress, ' PLZ ',' PLAZA ');
update addresses set fulldisplayaddress = replace(fulldisplayaddress, ' TER ',' TERRACE ');
update addresses set fulldisplayaddress = replace(fulldisplayaddress, ' ALY ',' ALLEY ');

--create final table
drop table if exists addresses_processed;
create temporary table addresses_processed on commit preserve rows as (
	select incident_id, call_date, original_address,trim(fulldisplayaddress) as fulldisplayaddress
	from addresses
	where fulldisplayaddress not ilike '%\&%'
UNION 
	select incident_id, call_date, original_address,trim(left(fulldisplayaddress,strpos(fulldisplayaddress,'&')-1)) as fulldisplayaddress
	from addresses
	where fulldisplayaddress ilike '%\&%' 
UNION 
	select incident_id, call_date, original_address,trim(right(fulldisplayaddress,length(fulldisplayaddress)-strpos(fulldisplayaddress,'&'))) as fulldisplayaddress
	from addresses
	where fulldisplayaddress ilike '%\&%' 
) with data;

--add common variations on "Capitol St" and MLK Jr Ave
insert into addresses_processed
SELECT incident_id, call_date, original_address, replace(fulldisplayaddress, 'CAPITOL','CAPITAL')
from addresses_processed
where fulldisplayaddress like '%CAPITOL%';

insert into addresses_processed
SELECT incident_id, call_date, original_address, replace(fulldisplayaddress, 'KING JR','KING JUNIOR')
from addresses_processed
where fulldisplayaddress like '%KING JR%';

insert into addresses_processed
SELECT incident_id, call_date, original_address, replace(fulldisplayaddress, 'LUTHER KING JR','LUTHER KING')
from addresses_processed
where fulldisplayaddress like '%KING JR%';

insert into addresses_processed
SELECT incident_id, call_date, original_address, 'SEA STREET'
from addresses_processed
where fulldisplayaddress = 'C STREET';

insert into addresses_processed
SELECT incident_id, call_date, original_address, 'SEE STREET'
from addresses_processed
where fulldisplayaddress = 'C STREET';


--delete the ones that are too short to be useful for joining
delete from addresses_processed where length(fulldisplayaddress)<=4;

--------------------------------------------------------------
--Prepare the Openmhz data
--Step 1: Link in unit numbers and addresses where possible
----------------------------------------------------------------

--Remove punctuation from the transcript
--Add responding unit numbers
drop table if exists unit_numbers;
create temporary table unit_numbers on commit preserve rows as (
	select 
		a.call_id
		, a.call_timestamp
		, a.call_length
		, replace(replace(replace(a.main_transcript::text,'.',''),',',''),'"',' ') as main_transcript_cleaned
		,main_transcript::varchar as main_transcript
		, array_agg(distinct b.unit_number::text) as unit_numbers_array
	from source_data.openmhz a
	left join (select *
			   , unnest(unit_number_audio) as audio_strings 
			   from source_data.dcfems_unit_numbers_lookup) b 
	on replace(replace(a.main_transcript::text,'.',''),',','') ilike concat('%',audio_strings::text,'%')
	where a.call_talkgroup = '101' --dispatcher talkgroup
		and call_timestamp::date >= '2021-05-15'--date automatic scraping started
	group by a.call_id, a.call_length, a.call_timestamp, replace(replace(replace(a.main_transcript::text,'.',''),',',''),'"',''),main_transcript::varchar
	) with data;

--Link in quadrants
drop table if exists call_quadrants;
create temporary table call_quadrants on commit preserve rows as (
	select 
		a.call_id
		, a.call_timestamp
		, a.call_length
		, a.main_transcript
		, a.main_transcript_cleaned
		, a.unit_numbers_array
		,array_agg(distinct b.quadrant::text) as call_quadrants
	from unit_numbers a
	left join quadrants b on call_date = call_timestamp::date and main_transcript_cleaned ilike concat('%',b.quadrant,'%') 
	group by  a.call_id, a.call_timestamp, a.call_length, a.main_transcript, a.main_transcript_cleaned, a.unit_numbers_array
	) with data;

--Link in addresses
drop table if exists call_addresses;
create temporary table call_addresses on commit preserve rows as (
	select 
		a.call_id
		, a.call_timestamp
		, a.call_length
		, a.main_transcript
		, a.main_transcript_cleaned
		, a.unit_numbers_array
		,a.call_quadrants
		,array_agg(distinct b.fulldisplayaddress::text) as call_addresses
	from call_quadrants a
	left join addresses_processed b on call_date = call_timestamp::date and main_transcript_cleaned ilike concat('%',' ',b.fulldisplayaddress,'%') 
	group by  a.call_id, a.call_timestamp, a.call_length, a.main_transcript, a.main_transcript_cleaned, a.unit_numbers_array, a.call_quadrants
	) with data;

---------------------------------------------------------------------------
--Step 2: Classify calls as car-crash related and/or crashes where someone
--outside of a car was hit
---------------------------------------------------------------------------

drop table if exists flag_pedestrians;
create temporary table flag_pedestrians on commit preserve rows as (
	--checks for "pedestrian" "cyclist"
	select distinct call_id
	,main_transcript_cleaned
	,regexp_matches(main_transcript_cleaned
				   ,'pedestrian|((?<!motor)cyclist)|the austrian'
				   ,'ig'
				   )
	from unit_numbers
	UNION 
	--checks for variations on "pedestrian/cyclist struck" 
	select distinct call_id
		,main_transcript_cleaned
		,regexp_matches(main_transcript_cleaned
						,'((charles and my son)|industry|equestrian|participant|(the restaurants)|kardashian|(dash three)|(that story)|british|(that.{0,10}your)|condition|addition|national|elizabeth|basically|destin((ation){0,1}|y{0,1})|destry|actually|traditional|potentially|expedition|professional|definitely|credential|possession|refreshing|petition|protection|production|reductionist|investment|investing|((?<!motor)cycl))\D{0,10}?(instruct|truck|drug|stroke|interrupt)'
						,'ig'
					   )
		from unit_numbers
	UNION 
	--checks for variations on "involving pedestrian/cyclist"
	select distinct call_id
		,main_transcript_cleaned
		,regexp_matches(main_transcript_cleaned
					   ,'involv.{0,20}?((charles and my son)|industry|equestrian|participant|correction|kardashian|addition|condition|position|destin|destry|traditional|potentially|expedition|professional|definitely|credential|possession|refreshing|petition|protection|production|reductionist|investment|investing|((?<!motor)cycl))'
					   ,'ig'
					   )
	from unit_numbers
	UNION
	--checks for variations on "struck by vehicle/car"
	select distinct call_id
		,main_transcript_cleaned
		,regexp_matches(main_transcript_cleaned
					   ,'((s.{0,1}?truck|drug)|stroke|hit).{1,3}?by.{1,3}?(vehicle|((metro){0,1}? {0,1}?bus)|car|motor)'
					   ,'ig'
					   )
	from unit_numbers
	) with data;

drop table if exists flag_accidents;
create temporary table flag_accidents on commit preserve rows as (
	--checks for all variations on MVA/MVC
	select distinct call_id
		,main_transcript_cleaned
		,regexp_matches(main_transcript_cleaned
					   ,'(((M|N) {0,1}?(V|B|C|T))|((e|E)nvy)).{0,3}?((a|A)ction|(a|A)ccent|(a|A)ctive|(a|A)ccident|A |C |sea|see)'
					   ,'g'
					   )
	from unit_numbers
	UNION 
	--checks for the "accident with injuries" phrase, "rollover"/"roll over", "vehicle accident", "motorcycle"
	select distinct call_id
		,main_transcript_cleaned
		,regexp_matches(main_transcript_cleaned
					   ,'((accident|active|action|accent)\D{0,15}?(injur|andrew|andrea))|(roll.{0,1}?over)|vehicle (accident|active|action|accent)|motor relax|overturned vehicle'
					   ,'ig'
					   )
	from unit_numbers
	UNION 
	--checks for car/vehicle/truck into a pole/tree
	select distinct call_id
	,main_transcript_cleaned
	,regexp_matches(main_transcript_cleaned
				   ,'(car|vehicle|truck).{0,5}?into a.{0,5}?(pole|tree|building)'
				   ,'ig'
				   )
	from unit_numbers
) with data;

drop table if exists flag_motorcycles;
create temporary table flag_motorcycles on commit preserve rows as (
	--just the phrase motorcycle
	select distinct call_id
	,main_transcript_cleaned
	,regexp_matches(main_transcript_cleaned
				   ,'motor.{0,1}?cycl'
				   ,'ig'
				   )
	from unit_numbers
) with data;

--exclude calls that might match a pedestrian regex but are definitely not pedestrian calls
drop table if exists flag_not_pedestrians;
create temporary table flag_not_pedestrians on commit preserve rows as (
	--struck by train
	select distinct call_id
	,main_transcript_cleaned
	,regexp_matches(main_transcript_cleaned
				   ,'(struck by.{0,3}?train)'
				   ,'ig'
				   )
	from unit_numbers
) with data;


--Combine all flags
drop table if exists classify_calls;
create temporary table classify_calls on commit preserve rows as (
select distinct a.*
	,case when mva.call_id is not null or peds.call_id is not null or motorcycles.call_id is not null then 1 else 0 end as car_crash_call
	,case when peds.call_id is not null and not_peds.call_id is null then 1 else 0 end as someone_outside_car_struck
	,case when motorcycles.call_id is not null then 1 else 0 end as motorcycle_flag
from call_addresses a
	left join flag_pedestrians peds on a.call_id = peds.call_id
	left join flag_accidents mva on a.call_id = mva.call_id
	left join flag_motorcycles motorcycles on a.call_id = motorcycles.call_id
	left join flag_not_pedestrians not_peds on not_peds.call_id = a.call_id
	) with data;


---------------------------------------------------------------------------
--Actually join
---------------------------------------------------------------------------

--Round 1: Calls where the arrays of responding unit numbers and addresses both overlap (highest confidence)
--Time window: -1 min/+ 15 min
drop table if exists join_step1;
create temporary table join_step1 on commit preserve rows as (
	select distinct a.*
	,b.call_id
	,b.call_timestamp
	,b.main_transcript_cleaned
	,b.call_length
	,b.unit_numbers_array
	,b.call_addresses
	,b.car_crash_call
	,b.someone_outside_car_struck 
	,b.motorcycle_flag
	,1 as join_round
	,'unit number and address' as join_type
from tmp_pulsepoint a
	inner join addresses_processed adr on a.incident_id = adr.incident_id
	inner join unit_ids units on a.incident_id = units.incident_id
	inner join classify_calls b on 
		--time
		extract(epoch from b.call_timestamp - a.call_received_datetime)/60 between -1.000 and 15.000
		--unit id
		and units.unit_id = ANY(b.unit_numbers_array) and units.call_date = b.call_timestamp::date
		--address
		and adr.fulldisplayaddress = ANY(b.call_addresses)
		--same date. may exclude some calls right at midnight but without an "equals" in the join condition somewhere it takes an hour
		and adr.call_date = b.call_timestamp::date
	) with data;
--8678

--Round 2: Matching on just unit numbers
--Exclude incidents that have already been matched with high confidence
--Do not exclude any calls - some calls contain multiple incidents and will match more than once
insert into join_step1
select distinct a.*
	,b.call_id
	,b.call_timestamp
	,b.main_transcript_cleaned
	,b.call_length
	,b.unit_numbers_array
	,b.call_addresses
	,b.car_crash_call
	,b.someone_outside_car_struck
	,b.motorcycle_flag
	,2 as join_round
	,'unit number' as join_type
from tmp_pulsepoint a
	inner join unit_ids units on a.incident_id = units.incident_id
	inner join classify_calls b on 
		--time
		extract(epoch from b.call_timestamp - a.call_received_datetime)/60 between -1.000 and 15.000
		--unit id
		and units.unit_id = ANY(b.unit_numbers_array) and units.call_date = b.call_timestamp::date
where a.incident_id not in (select distinct incident_id from join_step1)  --leave out incidents that already joined on both unit number and address
--8326

--Round 3: Matching on just address
--Exclude incidents that have already been matched using a unit number or unit number + address
--Do not exclude any calls - some calls contain multiple incidents and will match more than once
--Only allow "mismatched" joins (eg, where arrays are not null but also don't overlap) for the audio files over 25 seconds 
-- which are the ones most likely to contain multiple dispatches in the same audio file
insert into join_step1
select distinct a.*
	,b.call_id
	,b.call_timestamp
	,b.main_transcript_cleaned
	,b.call_length
	,b.unit_numbers_array
	,b.call_addresses
	,b.car_crash_call
	,b.someone_outside_car_struck
	,b.motorcycle_flag
	,3 as join_round
	,'address' as join_type
from tmp_pulsepoint a
	inner join addresses_processed adr on a.incident_id = adr.incident_id
	inner join classify_calls b on 
		--time
		extract(epoch from b.call_timestamp - a.call_received_datetime)/60 between -1.000 and 15.000
		--address
		and adr.fulldisplayaddress = ANY(b.call_addresses) and adr.call_date = b.call_timestamp::date
where a.incident_id not in (select distinct incident_id from join_step1) --leave out incidents that already joined on unit number or unit number plus address
	and (b.unit_numbers_array[1] is null or b.call_length>=25) --allow non-overlapping joins for long calls
--4561

--Round 4: Same quadrant and call type -- car crash calls only allowed to join to car crash incidents
--Pedestrian struck calls also allowed to join to medical emergencies (ME) or unknown emergencies (EM)
--Time window is now much more narrow: -30 seconds to + 1 min 30 seconds
--Exclude incidents that already matched on unit number
--Don't exclude incidents that only matched on address - that's less reliable
--Only match calls that either don't have unit numbers/addresses OR are over 25 seconds
--delete from join_step1 where join_round = 4
insert into join_step1
	select distinct a.*
	,b.call_id
	,b.call_timestamp
	,b.main_transcript_cleaned
	,b.call_length
	,b.unit_numbers_array
	,b.call_addresses
	,b.car_crash_call
	,b.someone_outside_car_struck
	,b.motorcycle_flag
	,4 as join_round
	,'quadrant and call type' as join_type
from tmp_pulsepoint a
	inner join quadrants quadrants on a.incident_id = quadrants.incident_id
	inner join classify_calls b on 
		--time
		extract(epoch from b.call_timestamp - a.call_received_datetime)/60 between -0.5 and 1.5
		--quadrant
		and quadrants.quadrant = ANY(b.call_quadrants) and quadrants.call_date = b.call_timestamp::date
		--incident type
		and (case when a.incident_type in ('TC', 'TCE', 'TCS', 'VF') then 1 else 0 end = b.car_crash_call
		 or (a.incident_type in ('ME', 'EM') and b.someone_outside_car_struck = 1))
where a.incident_id not in (select distinct incident_id from join_step1 where join_round in (1,2)) --leave out incidents that already joined on unit number or unit number + address
and ((b.call_addresses[1] is null and b.unit_numbers_array[1] is null) or b.call_length>=25) --allow non-overlapping joins for long calls
--4222

--Next round, call type only -- car crash calls only allowed to join to car crash incidents
--Pedestrian struck calls also allowed to join to medical emergencies (ME) or unknown emergencies (EM)
--Time window -30 seconds to + 1 min 30 seconds
--Exclude incidents that already matched on address and/or unit number
--Only match calls that either don't have unit numbers/addresses/quadrants OR are over 25 seconds
insert into join_step1
	select distinct a.*
	,b.call_id
	,b.call_timestamp
	,b.main_transcript_cleaned
	,b.call_length
	,b.unit_numbers_array
	,b.call_addresses
	,b.car_crash_call
	,b.someone_outside_car_struck
	,b.motorcycle_flag
	,5 as join_round
	,'call type only' as join_type
from tmp_pulsepoint a
	inner join classify_calls b on 
		--time
		extract(epoch from b.call_timestamp - a.call_received_datetime)/60 between -0.5 and 1.5
		--incident type
		and (case when a.incident_type in ('TC', 'TCE', 'TCS', 'VF') then 1 else 0 end = b.car_crash_call
		 or (a.incident_type in ('ME', 'EM') and b.someone_outside_car_struck = 1))
where a.incident_id not in (select distinct incident_id from join_step1 where join_round in (1,2)) --exclude incidents that already matched based on unit number and/or address
and ((b.call_addresses[1] is null and b.unit_numbers_array[1] is null and b.call_quadrants[1] is null) or b.call_length>=25)--allow non-overlapping joins for long calls
--4829

--Final final round, time only
--Time window 0 - 1 min
--Exclude incidents and also calls that already matched
insert into join_step1
	select distinct a.*
	,b.call_id
	,b.call_timestamp
	,b.main_transcript_cleaned
	,b.call_length
	,b.unit_numbers_array
	,b.call_addresses
	,b.car_crash_call
	,b.someone_outside_car_struck
	,b.motorcycle_flag
	,6 as join_round
	,'time only' as join_type
from tmp_pulsepoint a
	inner join classify_calls b on 
		--time
		extract(epoch from b.call_timestamp - a.call_received_datetime)/60 between 0 and 1.0
where a.incident_id not in (select distinct incident_id from join_step1) 
and b.call_id not in (select distinct call_id from join_step1)
--697

-------------------------------------------------------------------------
--Process the results
-------------------------------------------------------------------------

drop table if exists join_step2;
create temporary table join_step2 on commit preserve rows as (
	Select *
	,max(case when incident_type in ('TC', 'TCE', 'TCS', 'VF') then 1 else 0 end) over (partition by call_id) as Call_Matches_Car_Crash_Incident
	,max(car_crash_call) over (partition by incident_id) as Incident_Matches_Car_Crash_Call
	,max(case when car_crash_call = 0 then 1 else 0 end) over (partition by incident_id) as Incident_Matches_Non_Car_Crash_Call
	,min(case when car_crash_call = 0 then join_round else null end) over (partition by incident_id) as Non_Car_Crash_Call_Min_Join_Round
	,min(case when car_crash_call = 1 then join_round else null end) over (partition by incident_id) as Car_Crash_Call_Min_Join_Round
	,max(someone_outside_car_struck) over (partition by incident_id) as Incident_Matches_Ped_Struck_Call
	,min(join_round) over (partition by call_id) as call_min_join_round
	,min(join_round) over (partition by incident_id) as incident_min_join_round
	,dense_rank() over (partition by call_id order by abs(extract(epoch from call_timestamp - call_received_datetime))) as time_delta_rank_audio
	,dense_rank() over (partition by incident_id order by abs(extract(epoch from call_timestamp - call_received_datetime))) as time_delta_rank_pulsepoint
	,dense_rank() over (partition by call_id, join_round order by abs(extract(epoch from call_timestamp - call_received_datetime))) as time_delta_rank_audio_round
	,dense_rank() over (partition by incident_id, join_round order by abs(extract(epoch from call_timestamp - call_received_datetime))) as time_delta_rank_pulsepoint_round
	from join_step1
	) with data;
--31313

drop table if exists matches;
create temporary table matches on commit preserve rows as (
select * from join_step2
where join_round in (1,2)
and someone_outside_car_struck = 1
and (incident_type in ('TC', 'TCE', 'TCS', 'VF') 
	 or (Incident_type in ('ME', 'EM') 
		 and Call_Matches_Car_Crash_Incident = 0
		and Incident_Matches_Non_Car_Crash_Call = 0)
	)
and (time_delta_rank_pulsepoint_round = 1 or time_delta_rank_audio_Round=1)
) with data; 
--194
--now its 187?? why does it keep going down??

insert into matches
select * 
from join_step2 where someone_outside_car_struck = 1
and incident_id not in (select incident_id from matches)
and (incident_type in ('TC', 'TCE', 'TCS', 'VF') or (Incident_type in ('ME', 'EM') and Call_Matches_Car_Crash_Incident = 0
													and Incident_Matches_Non_Car_Crash_Call = 0))	
and (time_delta_rank_pulsepoint_round = 1 or (join_round<6 and time_delta_rank_audio_Round=1))
and join_round = call_min_join_round 
and call_min_join_round<=incident_min_join_round
--23

insert into matches
select * 
from join_step2 where someone_outside_car_struck = 1
and incident_id not in (select incident_id from matches)
and call_id not in (select call_id from matches)
and (incident_type in ('TC', 'TCE', 'TCS', 'VF') or (Incident_type in ('ME', 'EM') and Call_Matches_Car_Crash_Incident = 0
													and Incident_Matches_Non_Car_Crash_Call = 0))	
and (time_delta_rank_pulsepoint_round = 1 or (join_round<6 and time_delta_rank_audio_Round=1))
and join_round = incident_min_join_round 
and call_min_join_round<=incident_min_join_round
--0

--------------------------------
--non-pedestrian traffic calls
--------------------------------
--need to check 948973252...
insert into matches
select * from join_step2
where join_round in (1,2)
and incident_type in ('TC', 'TCE', 'TCS', 'VF') 
and (time_delta_rank_pulsepoint_round = 1 or time_delta_rank_audio_Round=1)
and incident_id not in (select incident_id from matches)
and call_id not in (select call_id from matches)
--ok that looks good
--1230

insert into matches
select *
from join_step2 
where 
 incident_id not in (select incident_id from matches)
 and call_id not in (select call_id from matches)
and incident_type in ('TC', 'TCE', 'TCS', 'VF') 
and (time_delta_rank_pulsepoint_round = 1 or (join_round<6 and time_delta_rank_audio_Round=1))
and join_round = call_min_join_round 
and call_min_join_round<=incident_min_join_round
order by incident_id
--290

insert into matches
select *
from join_step2 
where 
 incident_id not in (select incident_id from matches)
 and call_id not in (select call_id from matches)
and incident_type in ('TC', 'TCE', 'TCS', 'VF') 
and (time_delta_rank_pulsepoint_round = 1 or (join_round<6 and time_delta_rank_audio_Round=1))
and join_round = incident_min_join_round 
and call_min_join_round<=incident_min_join_round
order by incident_id
--11

drop table if exists final_match;
create temporary table final_match on commit preserve rows as (
select
		incident_id
		,incident_type
		,call_received_datetime
		,fulldisplayaddress
		,geography
		,max(someone_outside_car_struck) as someone_outside_car_struck
		,max(motorcycle_flag) as motorcycle_flag
		,max(car_crash_call) as car_crash_call
		,ARRAY_AGG(DISTINCT replace(call_id,'"','')) as call_ids_array
		,ARRAY_AGG(DISTINCT main_transcript_cleaned) as transcripts_array
	from matches
	group by 
		incident_id
		,incident_type
		,call_received_datetime
		,fulldisplayaddress
		,geography
) with data; 
--1491

insert into final_match
select incident_id
		,incident_type
		,call_received_datetime
		,fulldisplayaddress
		,geography
		,0 as someone_outside_car_struck
		,0 as motorcycle_flag
		,0 as car_crash_call
		,ARRAY[NULL] as call_ids_array
		,ARRAY[NULL] as transcripts_array
from tmp_pulsepoint
where incident_id not in (select incident_id from final_match)
and incident_type in ('TC', 'TCE', 'TCS', 'VF') 
--79

select * from source_data.waze_alerts_stream order by scrape_datetime desc limit 100;

drop table if exists tmp.final_scanner_audio_matches;
create table tmp.final_scanner_audio_matches as 
select * from final_match
--1600
---------------------------------------------------------------------
--join to Citizen data
---------------------------------------------------------------------

drop table if exists citizen_join_1;
create temporary table citizen_join_1 on commit preserve rows as (
select * from (
	select distinct a.*
		,b.geography as citizen_geography
		,ST_Distance(a.geography, b.geography) as Distance_To_Citizen_Incident
		,row_number() over (partition by a.incident_id order by ST_Distance(a.geography, b.geography)) as Distance_Rank
		,row_number() over (partition by b.incident_key order by ST_Distance(a.geography, b.geography)) as Citizen_Distance_Rank
		,abs(extract(epoch from b.cs - a.call_received_datetime)/60.00 ) as Minutes_Apart
		,b.incident_key
		,b.incident_desc_raw
		,b.motorcycle_flag as citizen_motorcycle_flag
		,b.someone_outside_car_struck as citizen_someone_outside_car_struck
	from tmp.final_scanner_audio_matches a
	left join source_data.citizen b on ST_DWITHIN(a.geography, b.geography, 500) 
		and abs(extract(epoch from b.cs - a.call_received_datetime)/60 )<60
	) as tmp where (Citizen_Distance_Rank = 1 and Distance_Rank = 1) or incident_key is null
) with data; 
--1560


---------------------------------------------------------------------
--join to twitter
---------------------------------------------------------------------

drop table if exists tmp_twitter;
create temporary table tmp_twitter on commit preserve rows as (
select * from source_data.twitter_stream
where ((tweet_text ilike '%pedestrian%' and tweet_text not ilike '%pedestrian bridge%')
	or tweet_text ilike '% cyclist%'
	   or tweet_text ilike '%bicycl%'
	   or tweet_text ilike '%ped struck%'
or ((tweet_text ilike '%struck by%' or tweet_text ilike '%hit by%')
	and tweet_text not ilike '%gunfire%' 
	and tweet_text not ilike '%bullets%'
   and tweet_text not ilike '%train%'
   ))
and created_at::date >='2021-05-15' and point_geography is not null
	) with data;
--58

select * from tmp.twitter_pulsepoint_join_final
where Ped_Crash_Any_Source = 1

select * from tmp_twitter
select * from citizen_join_1 where call_received_datetime between '2021-05-25 17:00:45-04' and '2021-05-25 19:15:45-04'
order by call_received_datetime
select * from tmp_crashes where fromdate = '2021-05-25'

drop table if exists twitter_join_1;
create temporary table twitter_join_1 on commit preserve rows as (
select * from (
	select distinct a.*
		,b.point_geography as twitter_geography
		,ST_Distance(a.geography, b.point_geography) as Distance_To_Tweet
		,row_number() over (partition by a.incident_id order by ST_Distance(a.geography, b.point_geography)) as Incident_Distance_Rank
		,row_number() over (partition by b.tweet_id order by ST_Distance(a.geography, b.point_geography)) as Twitter_Distance_Rank
		,(extract(epoch from b.created_at - a.call_received_datetime)/60.00 ) as Tweet_Minutes_Apart
		,b.tweet_id
		,b.tweet_text
		,case when b.tweet_id is not null then 1 else 0 end as twitter_someone_outside_car_struck
	from citizen_join_1 a
	left join tmp_twitter b on (ST_DWITHIN(a.geography, b.point_geography, 1000) 
								 or (ST_Area(b.polygon_geography::geography)<= 3000000 and ST_Intersects(b.polygon_geography, a.geography)))
		and (extract(epoch from b.created_at - a.call_received_datetime)/60.00 )between 0 and 30
	) as tmp where (Twitter_Distance_Rank = 1 and Incident_Distance_Rank = 1) or tweet_id is null
) with data; 
---1560

---------------------------------------------------------------------
--join to police-reported crashes
---------------------------------------------------------------------

--first create police crashes table

drop table if exists tmp_crashes;
create temporary table tmp_crashes on commit preserve rows as (
	select distinct 
		crimeid
	,fromdate::date
	,reportdate
	,address
	,total_vehicles
	,total_bicyclists
	,total_pedestrians
	,total_injuries
	,persontype_array
	,invehicletype_array
	,geography
	,pp_agency_incident_id
	,case when invehicletype_array::text ilike '%moped%' or invehicletype_array::text ilike '%motor%cycle%' or invehicletype_array::text ilike '%scooter%'
	then 1 else 0 end as total_non_car_vehicles
	,case when invehicletype_array::text ilike '%motor%cycle%' 
	then 1 else 0 end as motorcycle_flag
	,left(anc_id,1) as ward
	,case when total_bicyclists > 0 or total_pedestrians > 0 or invehicletype_array::text ilike '%moped%'or invehicletype_array::text ilike '%scooter%'
	then 1 else 0 end as person_outside_car_struck
	from analysis_data.dc_crashes_w_details
	where fromdate::date >= '2021-05-15'
) with data;
--3099


drop table if exists step1;
create temporary table step1 on commit preserve rows as (
	select * from (
	select distinct a.*
	,b.fromdate
	,b.reportdate
	,b.crimeid
	,b.address
	,b.total_vehicles
	,b.total_bicyclists as crash_total_bicyclists
	,b.total_pedestrians as crash_total_pedestrians
	,b.total_injuries
	,b.persontype_array
	,b.invehicletype_array
	,b.total_non_car_vehicles as crash_total_non_car_vehicles
	,b.person_outside_car_struck
	,b.motorcycle_flag as police_reports_motorcycle_flag
	,b.geography as crash_geo
	--flags: does the 911 call match to any crash with matching ped flags
	,MAX(case when (a.someone_outside_car_struck = 0 and a.citizen_someone_outside_car_struck = 0 and twitter_someone_outside_car_struck = 0) then null 
		 when (a.someone_outside_car_struck =1 or a.citizen_someone_outside_car_struck=1 or twitter_someone_outside_car_struck=1) and b.person_outside_car_struck =1 
		 then 1 else 0 end) over (Partition by a.incident_id) as Incident_Matches_Ped_Police_Report
	,row_number() over (partition by a.incident_id order by ST_Distance(a.geography, b.geography)) as Incident_to_Police_Report_Distance_Rank
	--call distance and time to report
	,(b.reportdate at time zone 'America/New_York')  - (a.CALL_RECEIVED_DATETIME at time zone 'America/New_York') as Time_To_Report
	,ST_Distance(a.geography, b.geography) as Call_Distance
	--min distance all crashes
	,min(ST_Distance(a.geography, b.geography)) over (partition by a.incident_id) as Min_Distance_To_Police_Crash
	--num matching reports	
	,sum(case when b.crimeid is not null then 1 else 0 end) over (Partition by a.incident_id) as Num_Matching_Police_Reports
	,sum(case when (a.someone_outside_car_struck =1 or a.citizen_someone_outside_car_struck=1 or twitter_someone_outside_car_struck=1) and b.person_outside_car_struck =1  then 1 else 0 end) over (partition by a.incident_id) as Num_Matching_Police_Reports_Same_Ped_Flag
	FROM twitter_join_1 a
        INNER JOIN tmp_crashes b on ST_DWITHIN(a.geography, b.geography, 200) 
            AND fromdate::date =cast((call_received_datetime at time zone 'America/New_York') as date)
            AND (a.CALL_RECEIVED_DATETIME at time zone 'America/New_York')  < (b.reportdate at time zone 'America/New_York') 
		) as tmp where (Incident_to_Police_Report_Distance_Rank = 1 or incident_id is null)
) with data;
--2011 left join
--3667 outer join
--3363 with the rank = 1
--3700
--935 with inner join

drop table if exists step2;
create temporary table step2 on commit preserve rows as (
	select * from (
		select *
		,row_number() over (partition by crimeid order by ST_Distance(geography, crash_geo)) as Police_Report_To_Incident_Distance_rank
		from step1
		) as tmp where (Police_Report_To_Incident_Distance_rank = 1 or crimeid is null)
	) with data;
--3273
--3650
--884


--try to fix instances where two vehicle collisions were dispatched in one audio file and one was a ped crash 
--and the other wasn't
drop table if exists exclude_call_ids;
create temporary table exclude_call_ids on commit preserve rows as (
	select ARRAY_AGG(call_ids) as exclude_call_ids FROM 
	(select unnest(call_ids_array) as call_ids from step2 where crimeid is not null and person_outside_car_struck = 1
INTERSECT
 select unnest(call_ids_array) as call_ids from step2 where crimeid is not null and person_outside_car_struck = 0 and 
 (someone_outside_car_struck =1 or citizen_someone_outside_car_struck=1 or twitter_someone_outside_car_struck=1)
) as tmp
	) with data;
----------------------------------------------------------------------------------
--FINALLY CATEGORIZE THE CALLS INTO A PERMANENT TABLE
----------------------------------------------------------------------------------

--first: The police-reported crashes with no 911 call
drop table if exists analysis_data.crashes_all_sources;
create table analysis_data.crashes_all_sources as
	(
	SELECT
		'MPD Only' as category
		,'' as sub_category
		,person_outside_car_struck as MPD_Reports_Ped_Involved
		,motorcycle_flag as MPD_Reports_Motorcycle_Involved
		,0 as Other_Sources_Report_Ped_Involved
		,crimeid as crash_id
		,NULL as incident_id
		,fromdate as accident_date
		,address as MPD_Reported_Address
		,NULL as DCFEMS_Call_Address
		,total_bicyclists as MPD_Reported_Bicyclists
		,total_pedestrians as MPD_Reported_Pedestrians
		,persontype_array
		,invehicletype_array
		,ARRAY[NULL] as scanner_audio
		,ARRAY[NULL] as scanner_call_ids
		,NULL as citizen_description
		,NULL as twitter_description
		,ST_Y(geography) as MPD_latitude
		,ST_X(geography) as MPD_longitude
		,geography::geography as MPD_Location
		,NULL::numeric as DCFEMS_Call_latitude
		,NULL::numeric as DCFEMS_Call_longitude
		,NULL::geography as DCFEMS_Call_Location
		,geography::geography as geography
	FROM tmp_crashes 
	WHERE crimeid not in (select crimeid from step2 where crimeid is not null)
	);
	grant all privileges on analysis_data.crashes_all_sources to public;

--67 
--then: The 911-reported crashes with no police report 
insert into analysis_data.crashes_all_sources
	SELECT
		'DCFEMS Only' as category
		,'' as sub_category
		,0 as MPD_Reports_Ped_Involved
		,0 as MPD_Reports_Motorcycle_Involved
		,case when (someone_outside_car_struck =1 or citizen_someone_outside_car_struck=1 or twitter_someone_outside_car_struck=1) then 1 else 0 end as Other_Sources_Report_Ped_Involved
		,NULL as crash_id
		,incident_id
		,call_received_datetime::date as accident_date
		,NULL as MPD_Reported_Address
		,fulldisplayaddress as DCFEMS_Call_Address
		,NULL as MPD_Reported_Bicyclists
		,NULL as MPD_Reported_Pedestrians
		,ARRAY[NULL] as persontype_array
		,ARRAY[NULL] as invehicletype_array
		,transcripts_array as scanner_audio
		,call_ids_array as scanner_call_ids
		,incident_desc_raw as citizen_description
		,tweet_text as twitter_description
		,NULL as MPD_latitude
		,NULL as MPD_longitude
		,NULL as MPD_Location
		,ST_Y(geography::geometry) as DCFEMS_Call_latitude
		,ST_X(geography::geometry) as DCFEMS_Call_longitude
		,geography as DCFEMS_Call_Location
		,geography
	FROM twitter_join_1 
	WHERE incident_id not in (select incident_id from step2 where incident_id is not null)
--90

--Calls that match
insert into analysis_data.crashes_all_sources
	SELECT
		'DCFEMS and MPD' as category
		,'No crash type conflict' as sub_category
		,person_outside_car_struck as MPD_Reports_Ped_Involved
		,police_reports_motorcycle_flag as MPD_Reports_Motorcycle_Involved
		,case when (someone_outside_car_struck =1 or citizen_someone_outside_car_struck=1 or twitter_someone_outside_car_struck=1)
			then 1 else 0 end as Other_Sources_Report_Ped_Involved
		,crimeid
		,incident_id
		,call_received_datetime::date as accident_date
		,address as MPD_Reported_Address
		,fulldisplayaddress as DCFEMS_Call_Address
		,crash_total_bicyclists as MPD_Reported_Bicyclists
		,crash_total_pedestrians as MPD_Reported_Pedestrians
		,persontype_array
		,invehicletype_array
		,transcripts_array as scanner_audio
		,incident_desc_raw as citizen_description
		,tweet_text as twitter_description
		,ST_Y(crash_geo) as MPD_latitude
		,ST_X(crash_geo) as MPD_longitude
		,crash_geo::geography as MPD_Location
		,ST_Y(geography::geometry) as DCFEMS_Call_latitude
		,ST_X(geography::geometry) as DCFEMS_Call_longitude
		,geography as DCFEMS_Call_Location
		,crash_geo::geography as geography
	FROM step2 
	WHERE  incident_id is not null and crimeid is not null and 
		(person_outside_car_struck = 1 or ((citizen_someone_outside_car_struck=0 or citizen_someone_outside_car_struck is null)
			and (twitter_someone_outside_car_struck=0 or twitter_someone_outside_car_struck is null)
										  and someone_outside_car_struck = 0))
--144

--Calls that match, but MPD doesn't think it's a ped crash but other sources do
insert into analysis_data.pedestrian_crashes_all_sources
	SELECT
		'DCFEMS and MPD' as category
		,'Other sources report a pedestrian or cyclist crash; MPD reports motorcycle involved' as sub_category
		,0 as MPD_Reports_Ped_Involved
		,1 as MPD_Reports_Motorcycle_Involved
		,1 as Other_Sources_Report_Ped_Involved
		,crimeid
		,incident_id
		,call_received_datetime::date as accident_date
		,address as MPD_Reported_Address
		,fulldisplayaddress as DCFEMS_Call_Address
		,crash_total_bicyclists as MPD_Reported_Bicyclists
		,crash_total_pedestrians as MPD_Reported_Pedestrians
		,persontype_array
		,invehicletype_array
		,transcripts_array as scanner_audio
		,incident_desc_raw as citizen_description
		,tweet_text as twitter_description
		,ST_Y(crash_geo) as MPD_latitude
		,ST_X(crash_geo) as MPD_longitude
		,crash_geo::geography as MPD_Location
		,ST_Y(geography::geometry) as DCFEMS_Call_latitude
		,ST_X(geography::geometry) as DCFEMS_Call_longitude
		,geography as DCFEMS_Call_Location
	FROM step2 
	WHERE  incident_id is not null and crimeid is not null and person_outside_car_struck = 0 
	and (someone_outside_car_struck =1 or citizen_someone_outside_car_struck=1 or twitter_someone_outside_car_struck=1)
	and police_reports_motorcycle_flag = 1
--9

--Calls that match, but MPD doesn't think it's a ped crash but other sources do, and motorcycle is not a possible reason for this
insert into analysis_data.crashes_all_sources
	SELECT
		'DCFEMS and MPD' as category
		,'Other sources report a pedestrian or cyclist crash; MPD reports no non-vehicle parties involved' as sub_category
		,0 as MPD_Reports_Ped_Involved
		,0 as MPD_Reports_Motorcycle_Involved
		,1 as Other_Sources_Report_Ped_Involved
		,crimeid
		,incident_id
		,call_received_datetime::date as accident_date
		,address as MPD_Reported_Address
		,fulldisplayaddress as DCFEMS_Call_Address
		,crash_total_bicyclists as MPD_Reported_Bicyclists
		,crash_total_pedestrians as MPD_Reported_Pedestrians
		,persontype_array
		,invehicletype_array
		,transcripts_array as scanner_audio
		,incident_desc_raw as citizen_description
		,tweet_text as twitter_description
		,ST_Y(crash_geo) as MPD_latitude
		,ST_X(crash_geo) as MPD_longitude
		,crash_geo::geography as MPD_Location
		,ST_Y(geography::geometry) as DCFEMS_Call_latitude
		,ST_X(geography::geometry) as DCFEMS_Call_longitude
		,geography as DCFEMS_Call_Location
	FROM step2 a
	left join exclude_call_ids b on a.call_ids_array && b.exclude_call_ids
	WHERE  incident_id is not null and crimeid is not null and person_outside_car_struck = 0 
	and (someone_outside_car_struck =1 or citizen_someone_outside_car_struck=1 or twitter_someone_outside_car_struck=1)
	and police_reports_motorcycle_flag = 0
	and b.exclude_call_ids is null
--31

--Car-only crashes in DCFEMS data and not MPD data

	SELECT
		'DCFEMS Only' as category
		,'' as sub_category
		,0 as MPD_Reports_Ped_Involved
		,0 as MPD_Reports_Motorcycle_Involved
		,0 as Other_Sources_Report_Ped_Involved
		,NULL as crash_id
		,incident_id
		,call_received_datetime::date as accident_date
		,NULL as MPD_Reported_Address
		,fulldisplayaddress as DCFEMS_Call_Address
		,NULL as MPD_Reported_Bicyclists
		,NULL as MPD_Reported_Pedestrians
		,ARRAY[NULL] as persontype_array
		,ARRAY[NULL] as invehicletype_array
		,transcripts_array as scanner_audio
		,incident_desc_raw as citizen_description
		,tweet_text as twitter_description
		,NULL as MPD_latitude
		,NULL as MPD_longitude
		,NULL as MPD_Location
		,ST_Y(geography::geometry) as DCFEMS_Call_latitude
		,ST_X(geography::geometry) as DCFEMS_Call_longitude
		,geography as DCFEMS_Call_Location
	FROM twitter_join_1 
	WHERE  someone_outside_car_struck =0 and 
			(citizen_someone_outside_car_struck=0 or citizen_someone_outside_car_struck is null)
			and (twitter_someone_outside_car_struck=0 or twitter_someone_outside_car_struck is null) 
	AND incident_id not in (select incident_id from step2 where incident_id is not null)

select count(*) from step2
--3650
select count(*) from analysis_data.pedestrian_crashes_all_sources
--341
where (category = 'DCFEMS Only' 
or sub_category = 'Other sources report a pedestrian or cyclist crash; MPD reports no non-vehicle parties involved')
--116

select category, sub_category, count(*)
from analysis_data.pedestrian_crashes_all_sources
group by category, sub_category
order by category, sub_category

select * from analysis_data.pedestrian_crashes_all_sources
where sub_category <>  'Other sources report a pedestrian or cyclist crash; MPD reports no non-vehicle parties involved'
and category <> 'DCFEMS Only'

select * from analysis_data.pedestrian_crashes_all_sources
where sub_category =  'Other sources report a pedestrian or cyclist crash; MPD reports no non-vehicle parties involved'
or category = 'DCFEMS Only'
-

select * from analysis_data.pedestrian_crashes_all_sources
where mpd_reports_ped_involved = 1 and other_sources_report_ped_involved = 0 and incident_id is not null
