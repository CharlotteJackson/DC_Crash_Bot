/********************************
QUESTIONS: 
    Which neighborhoods in DC have the highest rates of children under 12 hit by cars while walking? 
    Has this changed over time?
	Have residents of these neighborhoods asked DDOT to address dangerous road conditions?
Datasets used to find the answer: 
    source_data.address_points: Detailed information on every address in DC, including ward, neighborhood, ANC district, etc
    analysis_data.dc_crashes_w_details: Data on car crashes from Open Data DC, with some columns added for easier analysis
	source_data.all311: Open Data DC dataset on all Traffic Safety Assessment Requests submitted to 311 since 2015 
	source_data.vision_zero: Open Data DC dataset on all Vision Zero requests submitted
PostGIS functions used to find the answer:
    ST_ConcaveHull
    ST_Collect
    ST_Intersects
*******************************/

--Step 1: Generate neighborhood boundaries from the address dataset	
CREATE TEMP TABLE nabe_boundaries ON COMMIT PRESERVE ROWS AS (
SELECT 
    d.assessment_nbhd AS nabe,
	ST_ConcaveHull(ST_Collect(d.geometry), 0.99) AS geometry
FROM source_data.address_points AS d
GROUP BY d.assessment_nbhd
)
WITH DATA;
	
--Step 2: Add the neighborhood to each crash record
CREATE TEMP TABLE crashes_w_neighborhood  ON COMMIT PRESERVE ROWS AS (
SELECT b.nabe, a.*
FROM analysis_data.dc_crashes_w_details a
INNER JOIN nabe_boundaries b ON ST_Intersects(b.geometry, a.geometry)
) WITH DATA;

--Step 3: Calculate metric of interest
CREATE TEMP TABLE child_pedestrian_crashes  ON COMMIT PRESERVE ROWS AS (
SELECT 
	nabe
	,extract(YEAR from FROMDATE) as YEAR
	,COUNT(distinct ObjectID) as Total_Crashes
	,sum(PEDS_UNDER_12) as Total_Ped_Children
	,cast(sum(PEDS_UNDER_12*1.00)/COUNT(distinct ObjectID) as decimal(10,4)) AS Pct_Ped_Children
FROM crashes_w_neighborhood
GROUP BY nabe, extract(YEAR FROM FROMDATE)
ORDER BY extract(YEAR FROM FROMDATE) desc, Total_Crashes desc, Pct_Ped_Children desc 
) WITH DATA;

SELECT * FROM child_pedestrian_crashes;
--Randle Heights is a consistent problem spot for children on foot getting hit by cars

--Were there any traffic safety assessment requests submitted for this neighborhood?
CREATE TEMP TABLE all311_w_neighborhood  ON COMMIT PRESERVE ROWS AS (
SELECT b.nabe, a.*
FROM source_data.all311 a
INNER JOIN nabe_boundaries b ON ST_Intersects(b.geometry, a.geometry)
) WITH DATA;

SELECT * FROM all311_w_neighborhood WHERE nabe = 'Randle Heights';
--Yes, there have been 265 traffic safety assessment requests in this neighborhood since 2015

--How does this number of requests compare to other neighborhoods?
SELECT nabe, count(*) FROM all311_w_neighborhood
GROUP BY nabe order by count(*) DESC 
--It's the 7th highest neighborhood for all time TSA requests.

--How about Vision Zero requests submitted for this neighborhood?
CREATE TEMP TABLE vision_zero_w_neighborhood  ON COMMIT PRESERVE ROWS AS (
SELECT b.nabe, a.*
FROM source_data.vision_zero a
INNER JOIN nabe_boundaries b ON ST_Intersects(b.geometry, a.geometry)
) WITH DATA;

SELECT * FROM vision_zero_w_neighborhood WHERE nabe = 'Randle Heights';
--Yes, there have been 11 Vision Zero safety requests, two of which specifically call out danger to children
