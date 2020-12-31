/********************************
QUESTIONS: 
    Which neighborhoods in DC have the highest rates of children under 12 hit by cars while walking? 
    Has this changed over time?
	Have residents of these neighborhoods asked DDOT to address dangerous road conditions?
Datasets used to find the answer: 
    select * from source_data.address_points limit 1000: Detailed information on every address in DC, including ward, neighborhood, ANC district, etc
    select * from analysis_data.dc_crashes_w_details limit 1000: Data on car crashes from Open Data DC, with some columns added for easier analysis
	select * from source_data.all311: Open Data DC dataset on all Traffic Safety Assessment Requests submitted to 311 since 2015 
	select * from source_data.vision_zero: Open Data DC dataset on all Vision Zero requests submitted
PostGIS functions used to find the answer:
    ST_ConcaveHull
    ST_Collect
    ST_Intersects
*******************************/
select * from source_data.address_points limit 100;
--Step 1: Generate neighborhood boundaries from the address dataset	
CREATE TEMP TABLE nabe_boundaries ON COMMIT PRESERVE ROWS AS (
SELECT 
    d.assessment_nbhd AS nabe,
	ST_ConcaveHull(ST_Collect(d.geography::geometry), 0.99) AS geography
FROM source_data.address_points AS d
GROUP BY d.assessment_nbhd
)
WITH DATA;

select * from nabe_boundaries;
	
--Step 2: Add the neighborhood to each crash record
DROP TABLE crashes_w_neighborhood;
CREATE TEMP TABLE crashes_w_neighborhood  ON COMMIT PRESERVE ROWS AS (
SELECT b.nabe, b.geography as nabe_boundary, a.*
FROM analysis_data.dc_crashes_w_details a
INNER JOIN nabe_boundaries b ON ST_Intersects(b.geography::geometry, a.geography::geometry)
) WITH DATA;

select * from crashes_w_neighborhood limit 100;
select * from crashes_w_neighborhood where extract(YEAR from FROMDATE) = 2020 and nabe in ('Chevy Chase','Randle Heights') 
and total_pedestrians>0
order by nabe;
--both of those neighborhoods border MD and have major commuter routes running through them
--guess which one had literally 8x the number of crash reports!

--Step 3: Calculate metric of interest
DROP TABLE child_pedestrian_crashes;
CREATE TEMP TABLE child_pedestrian_crashes  ON COMMIT PRESERVE ROWS AS (
SELECT 
	nabe
	,nabe_boundary
	,extract(YEAR from FROMDATE) as YEAR
	,COUNT(distinct ObjectID) as Total_Crashes
	,sum(PEDS_UNDER_12) as Total_Ped_Children
	,cast(sum(PEDS_UNDER_12*1.00)/COUNT(distinct ObjectID) as decimal(10,4)) AS Pct_Ped_Children
FROM crashes_w_neighborhood
GROUP BY nabe,nabe_boundary, extract(YEAR FROM FROMDATE)
ORDER BY extract(YEAR FROM FROMDATE) desc, Total_Crashes desc, Pct_Ped_Children desc 
) WITH DATA;

SELECT * FROM child_pedestrian_crashes order by total_ped_children desc;
--Randle Heights is a consistent problem spot for children on foot getting hit by cars

--Were there any traffic safety assessment requests submitted for this neighborhood?
DROP TABLE all311_w_neighborhood;
CREATE TEMP TABLE all311_w_neighborhood  ON COMMIT PRESERVE ROWS AS (
SELECT b.nabe, a.*
FROM source_data.all311 a
INNER JOIN nabe_boundaries b ON ST_Intersects(b.geography::geometry, a.geography::geometry)
) WITH DATA;

SELECT * FROM all311_w_neighborhood WHERE nabe = 'Randle Heights';
--Yes, there have been 269 traffic safety assessment requests in this neighborhood since 2015

--How does this number of requests compare to other neighborhoods?
SELECT nabe, count(*) FROM all311_w_neighborhood
GROUP BY nabe order by count(*) DESC 
--It's the 6th highest neighborhood for all time TSA requests.

--How about Vision Zero requests submitted for this neighborhood?
DROP TABLE vision_zero_w_neighborhood;
CREATE TEMP TABLE vision_zero_w_neighborhood  ON COMMIT PRESERVE ROWS AS (
SELECT b.nabe, a.*
FROM source_data.vision_zero a
INNER JOIN nabe_boundaries b ON ST_Intersects(b.geography::geometry, a.geography::geometry)
) WITH DATA;

SELECT * FROM vision_zero_w_neighborhood WHERE nabe = 'Randle Heights';
--Yes, there have been 11 Vision Zero safety requests, two of which specifically call out danger to children

SELECT nabe, count(*) FROM vision_zero_w_neighborhood
GROUP BY nabe order by count(*) DESC 
