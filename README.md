# DC_Crash_Bot

DC Crash Bot has two goals:

1) Combine the city's open crash data with data on 311 and Vision Zero safety requests from residents to highlight the government's response (or lack thereof) to citizen concerns about the impact of traffic violence in their neighborhood
2) The open crashes dataset is only a start. We want to build on it by incorporating other sources, such as the Pulsepoint app for first responders, crash databases maintained by third parties, and Twitter, to get a fuller picture of the damage. 


### Why Start this project

DC's current Vision Zero program [is failing](https://mpdc.dc.gov/page/traffic-fatalities), and the incompleteness of the open crash data is [a known problem](https://www.washingtonpost.com/opinions/local-opinions/ignorance-is-not-bliss-for-the-safety-of-dc-bicyclists-and-pedestrians/2020/02/27/c9180e74-5276-11ea-b119-4faabac6674f_story.html) with serious consequences for city residents. 
https://usa.streetsblog.org/2020/09/29/why-your-city-doesnt-map-its-worst-car-crashes/


# Resources 

[Data dictionary for the analysis_data schema in our database](https://docs.google.com/spreadsheets/d/18OQh78KhlL65JHM2DyQKVEdYUY8zXjHzTfYbdsd-Kes/edit?usp=sharing) 

[The Google doc where we brainstorm analysis ideas](https://docs.google.com/document/d/1HlG7ByM-neLiwFWd1FnOOlRzC0llvIF79l7-fzT8WpE/edit?usp=sharing)

[Diagram of our data flow and architecture](https://docs.google.com/presentation/d/1QyD4gr7tRS95WkxUa_VGdnQ99jNrT60-43LnF4n9Z5M/edit?usp=sharing) (still a work in progress!)

### Link to our map

TODO:

### How to contribute
 
Our PostGIS database currently has Open Data DC datasets on 311/Vision Zero requests, crashes, crash details, census blocks, address points, and all roadway centerlines-related data. We also have a regular feed of DC Fire & EMS dispatch data for car crash calls.  **If you're interested in doing any reporting, analytics, or visualization with this data, all you need to do is 1) download pgAdmin; 2) message me for a login.** 

We need people with interests or skills in front-end, machine learning/NLP, data engineering, and geographic data. If that sounds like you, join us at an upcoming [Code for DC Meetup](https://www.meetup.com/Code-for-DC/)! 

#### Finding your way around

This repository contains multiple projects, which are run locally in different ways depending on their architecture. Check them out:

- [`/dcmap`](/dcmap) is the DC map front-end being worked on in [#90](https://github.com/CharlotteJackson/DC_Crash_Bot/issues/90)
- [`/frontend`](/frontend) is a map front-end, but isn’t actively being worked on at the moment.
- [`/data`](/data) contains exports from our main PostgreSQL database — feel free to poke around.
- [`/notebooks`](/notebooks) contain different analysis projects in the form of Jupyter Notebooks.
- [`/scripts`](/scripts) are miscellaneous data cleaning and collection python scripts.
- [`/SQL`](/SQL) contain miscellaneous SQL queries against our main PostgreSQL database.

### Sample query

```SQL
--Which DC neighborhood has submitted the most Traffic Safety Assessment requests since 2015? 
CREATE TABLE Neighborhoods  AS (
SELECT 
    assessment_nbhd AS Neighborhood,
	ST_ConcaveHull(ST_Collect(d.geometry), 0.99) AS geometry
FROM source_data.address_points 
GROUP BY assessment_nbhd
)

CREATE TABLE all311_w_neighborhood AS (
SELECT b.Neighborhood, a.*
FROM source_data.all311 a
INNER JOIN Neighborhoods b ON ST_Intersects(b.geometry, a.geometry)
) 

SELECT Neighborhood, count(*) FROM all311_w_neighborhood
GROUP BY Neighborhood order by count(*) DESC 

```
