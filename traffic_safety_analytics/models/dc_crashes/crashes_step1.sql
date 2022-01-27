{{ config(
    schema='tmp'
    ,materialized = 'table'
    ,indexes=[
      {'columns': ['geography'], 'type': 'GIST'},
      {'columns': ['crimeid'], 'unique': True},
    ]
    ) }}

  with crash_details as (
        SELECT
            objectid
            ,crimeid
            ,ccn
            ,personid
            ,persontype
            ,age::numeric as age
            ,fatal
            ,majorinjury
            ,minorinjury
            ,vehicleid
            ,invehicletype
            ,ticketissued
            ,licenseplatestate
            ,impaired
            ,speeding
        FROM source_data.crash_details
    )

    ,crashes_raw AS (
    SELECT
        u_objectid::varchar AS objectid, 
        u_crimeid::varchar AS crimeid, 
        u_ccn::varchar AS ccn, 
        u_reportdate::timestamptz AS reportdate, 
        u_routeid::varchar AS routeid, 
        u_measure::varchar AS measure, 
        u_offset::varchar AS _offset, 
        u_streetsegid::varchar AS streetsegid, 
        u_roadwaysegid::varchar AS roadwaysegid, 
        u_fromdate::date AS fromdate, 
        u_todate::date AS todate, 
        u_marid::varchar AS marid, 
        u_address::varchar AS address, 
        u_latitude::varchar AS latitude, 
        u_longitude::varchar AS longitude, 
        u_xcoord::varchar AS xcoord, 
        u_ycoord::varchar AS ycoord, 
        u_ward::varchar AS ward, 
        u_eventid::varchar AS eventid, 
        u_mar_address::varchar AS mar_address, 
        u_mar_score::varchar AS mar_score, 
        u_majorinjuries_bicyclist::int AS majorinjuries_bicyclist, 
        u_minorinjuries_bicyclist::int AS minorinjuries_bicyclist, 
        u_unknowninjuries_bicyclist::int AS unknowninjuries_bicyclist, 
        u_fatal_bicyclist::int AS fatal_bicyclist, 
        u_majorinjuries_driver::int AS majorinjuries_driver, 
        u_minorinjuries_driver::int AS minorinjuries_driver, 
        u_unknowninjuries_driver::int AS unknowninjuries_driver, 
        u_fatal_driver::int AS fatal_driver, 
        u_majorinjuries_pedestrian::int AS majorinjuries_pedestrian, 
        u_minorinjuries_pedestrian::int AS minorinjuries_pedestrian, 
        u_unknowninjuries_pedestrian::int AS unknowninjuries_pedestrian, 
        u_fatal_pedestrian::int AS fatal_pedestrian, 
        u_total_vehicles::int AS total_vehicles, 
        u_total_bicycles::int AS total_bicycles, 
        u_total_pedestrians::int AS total_pedestrians, 
        u_pedestriansimpaired::int AS pedestriansimpaired, 
        u_bicyclistsimpaired::int AS bicyclistsimpaired, 
        u_driversimpaired::int AS driversimpaired, 
        u_total_taxis::int AS total_taxis, 
        u_total_government::int AS total_government, 
        u_speeding_involved::int AS speeding_involved, 
        u_nearestintrouteid::varchar AS nearestintrouteid, 
        u_nearestintstreetname::varchar AS nearestintstreetname, 
        u_offintersection::varchar AS offintersection, 
        u_intapproachdirection::varchar AS intapproachdirection, 
        u_locationerror::varchar AS locationerror, 
        u_lastupdatedate::timestamptz AS lastupdatedate, 
        u_mpdlatitude::varchar AS mpdlatitude, 
        u_mpdlongitude::varchar AS mpdlongitude, 
        u_mpdgeox::varchar AS mpdgeox, 
        u_mpdgeoy::varchar AS mpdgeoy, 
        u_blockkey::varchar AS blockkey, 
        u_subblockkey::varchar AS subblockkey, 
        u_fatalpassenger::int AS fatalpassenger, 
        u_majorinjuriespassenger::int AS majorinjuriespassenger, 
        u_minorinjuriespassenger::int AS minorinjuriespassenger, 
        u_unknowninjuriespassenger::int AS unknowninjuriespassenger, 
        ST_Force2D(ST_GeomFromText(u_geometry, 4326)) as geography
    FROM source_data.crashes_raw
    )

    ,crash_details_new_columns
    AS (
        SELECT *
            ,CASE WHEN PERSONTYPE = 'Driver' AND AGE >=65 THEN 1 ELSE 0 END AS DRIVERS_OVER_65
            ,CASE WHEN PERSONTYPE = 'Driver' AND AGE <=25 THEN 1 ELSE 0 END AS DRIVERS_UNDER_25
            ,CASE WHEN PERSONTYPE = 'Pedestrian' AND AGE >=65 THEN 1 ELSE 0 END AS PEDS_OVER_65
            ,CASE WHEN PERSONTYPE = 'Pedestrian' AND AGE <=12 THEN 1 ELSE 0 END AS PEDS_UNDER_12
            ,CASE WHEN PERSONTYPE = 'Bicyclist' AND AGE >=65 THEN 1 ELSE 0 END AS BIKERS_OVER_65
            ,CASE WHEN PERSONTYPE = 'Bicyclist' AND AGE <=18 THEN 1 ELSE 0 END AS BIKERS_UNDER_18
            ,CASE WHEN PERSONTYPE = 'Driver' AND LICENSEPLATESTATE <> 'DC' AND LICENSEPLATESTATE <> ' None' THEN 1 ELSE 0 END AS OOS_VEHICLES
            ,CASE WHEN PERSONTYPE = 'Driver' AND INVEHICLETYPE = 'Passenger Car/automobile' THEN 1 ELSE 0 END AS NUM_CARS
            ,CASE WHEN PERSONTYPE = 'Driver' AND INVEHICLETYPE in ('Suv (sport Utility Vehicle)', 'Pickup Truck') THEN 1 ELSE 0 END AS NUM_SUVS_OR_TRUCKS

            ,CASE WHEN PERSONTYPE = 'Pedestrian' AND FATAL='Y' THEN 1 ELSE 0 END AS PED_FATALITIES
            ,CASE WHEN PERSONTYPE = 'Bicyclist' AND FATAL='Y'THEN 1 ELSE 0 END AS BICYCLE_FATALITIES
            ,CASE WHEN PERSONTYPE in ('Driver','Passenger') AND FATAL='Y' THEN 1 ELSE 0 END AS VEHICLE_FATALITIES

            ,CASE WHEN PERSONTYPE = 'Pedestrian' AND (MAJORINJURY='Y' OR MINORINJURY ='Y')THEN 1 ELSE 0 END AS PED_INJURIES
            ,CASE WHEN PERSONTYPE = 'Bicyclist' AND (MAJORINJURY='Y' OR MINORINJURY ='Y') THEN 1 ELSE 0 END AS BICYCLE_INJURIES
            ,CASE WHEN PERSONTYPE in ('Driver','Passenger') AND (MAJORINJURY='Y' OR MINORINJURY ='Y') THEN 1 ELSE 0 END AS VEHICLE_INJURIES
            ,CASE WHEN PERSONTYPE = 'Driver' AND TICKETISSUED ='Y' THEN 1 ELSE 0 END AS DRIVER_TICKETS
            ,CASE WHEN PERSONTYPE = 'Driver' AND SPEEDING ='Y' THEN 1 ELSE 0 END AS DRIVERS_SPEEDING
            ,CASE WHEN PERSONTYPE = 'Driver' AND IMPAIRED ='Y' THEN 1 ELSE 0 END AS DRIVERS_IMPAIRED

            ,CASE WHEN PERSONTYPE = 'Bicyclist' AND TICKETISSUED ='Y' THEN 1 ELSE 0 END AS BICYCLE_TICKETS
            ,CASE WHEN PERSONTYPE = 'Pedestrian' AND TICKETISSUED ='Y'  THEN 1 ELSE 0 END AS PED_TICKETS
            ,CASE WHEN (MAJORINJURY='Y' OR MINORINJURY ='Y') THEN 1 ELSE 0 END AS TOTAL_INJURIES
            ,CASE WHEN MAJORINJURY='Y' THEN 1 ELSE 0 END AS TOTAL_MAJOR_INJURIES
            ,CASE WHEN MINORINJURY ='Y' THEN 1 ELSE 0 END AS TOTAL_MINOR_INJURIES

            ,CASE WHEN PERSONTYPE = 'Driver' THEN 1 ELSE 0 END AS TOTAL_VEHICLES
            ,CASE WHEN PERSONTYPE = 'Pedestrian' THEN 1 ELSE 0 END AS TOTAL_PEDESTRIANS
            ,CASE WHEN PERSONTYPE = 'Bicyclist' THEN 1 ELSE 0 END AS TOTAL_BICYCLISTS
        FROM crash_details
    )
    
    ,crash_details_agg 
    AS (
        SELECT 
            CRIMEID
            ,SUM(DRIVERS_OVER_65) AS DRIVERS_OVER_65
            ,SUM(DRIVERS_UNDER_25) AS DRIVERS_UNDER_25
            ,SUM(PEDS_OVER_65) AS PEDS_OVER_65
            ,SUM(PEDS_UNDER_12) AS PEDS_UNDER_12
            ,SUM(BIKERS_OVER_65) AS BIKERS_OVER_65
            ,SUM(BIKERS_UNDER_18) AS BIKERS_UNDER_18
            ,SUM(OOS_VEHICLES) AS OOS_VEHICLES
            ,SUM(NUM_CARS) AS NUM_CARS
            ,SUM(NUM_SUVS_OR_TRUCKS) AS NUM_SUVS_OR_TRUCKS
            ,SUM(PED_INJURIES) AS PEDESTRIAN_INJURIES
            ,SUM(BICYCLE_INJURIES) AS BICYCLE_INJURIES
            ,SUM(VEHICLE_INJURIES) AS VEHICLE_INJURIES
            ,SUM(PED_FATALITIES) AS PEDESTRIAN_FATALITIES
            ,SUM(BICYCLE_FATALITIES) AS BICYCLE_FATALITIES
            ,SUM(VEHICLE_FATALITIES) AS VEHICLE_FATALITIES
            ,SUM(DRIVER_TICKETS) AS DRIVER_TICKETS
            ,SUM(DRIVERS_SPEEDING) AS DRIVERS_SPEEDING
            ,SUM(DRIVERS_IMPAIRED) AS DRIVERS_IMPAIRED
            ,SUM(BICYCLE_TICKETS) AS BICYCLE_TICKETS
            ,SUM(PED_TICKETS) AS PED_TICKETS
            ,SUM(TOTAL_INJURIES) AS TOTAL_INJURIES
            ,SUM(TOTAL_MAJOR_INJURIES) AS TOTAL_MAJOR_INJURIES
            ,SUM(TOTAL_MINOR_INJURIES) AS TOTAL_MINOR_INJURIES
            ,SUM(TOTAL_VEHICLES) AS TOTAL_VEHICLES
            ,SUM(TOTAL_PEDESTRIANS) AS TOTAL_PEDESTRIANS
            ,SUM(TOTAL_BICYCLISTS) AS TOTAL_BICYCLISTS
            ,ARRAY_AGG(PERSONTYPE) AS PERSONTYPE_ARRAY
            ,ARRAY_AGG(INVEHICLETYPE) AS INVEHICLETYPE_ARRAY
            ,ARRAY_AGG(LICENSEPLATESTATE) AS LICENSEPLATESTATE_ARRAY
        FROM crash_details_new_columns
        GROUP BY CRIMEID
    ) 

    ,crashes_join AS (
        SELECT 
            a.OBJECTID
                ,a.CRIMEID
                ,a.REPORTDATE
                ,a.FROMDATE
                ,a.TODATE 
                ,a.ADDRESS
                ,a.mpdlatitude
                ,a.mpdlongitude
                ,CASE WHEN b.CRIMEID IS NULL OR b.BICYCLE_INJURIES < (a.MAJORINJURIES_BICYCLIST + a.MINORINJURIES_BICYCLIST + a.UNKNOWNINJURIES_BICYCLIST)
                    THEN (a.MAJORINJURIES_BICYCLIST + a.MINORINJURIES_BICYCLIST + a.UNKNOWNINJURIES_BICYCLIST)
                    ELSE b.BICYCLE_INJURIES END AS BICYCLE_INJURIES
                ,CASE WHEN b.CRIMEID IS NULL OR b.VEHICLE_INJURIES < (a.MAJORINJURIES_DRIVER+a.MINORINJURIES_DRIVER+a.UNKNOWNINJURIES_DRIVER+a.MAJORINJURIESPASSENGER+a.MINORINJURIESPASSENGER+a.UNKNOWNINJURIESPASSENGER)
                    THEN (a.MAJORINJURIES_DRIVER+a.MINORINJURIES_DRIVER+a.UNKNOWNINJURIES_DRIVER+a.MAJORINJURIESPASSENGER+a.MINORINJURIESPASSENGER+a.UNKNOWNINJURIESPASSENGER)
                    ELSE b.VEHICLE_INJURIES END AS VEHICLE_INJURIES
                ,CASE WHEN b.CRIMEID IS NULL OR b.PEDESTRIAN_INJURIES < (a.MAJORINJURIES_PEDESTRIAN+ a.MINORINJURIES_PEDESTRIAN + a.UNKNOWNINJURIES_PEDESTRIAN)
                    THEN (a.MAJORINJURIES_PEDESTRIAN + a.MINORINJURIES_PEDESTRIAN + a.UNKNOWNINJURIES_PEDESTRIAN)
                    ELSE b.PEDESTRIAN_INJURIES END AS PEDESTRIAN_INJURIES
                ,CASE WHEN b.CRIMEID IS NULL OR b.TOTAL_INJURIES < (a.MAJORINJURIES_PEDESTRIAN+ a.MINORINJURIES_PEDESTRIAN + a.UNKNOWNINJURIES_PEDESTRIAN
                                                                    +a.MAJORINJURIES_DRIVER+a.MINORINJURIES_DRIVER+a.UNKNOWNINJURIES_DRIVER+a.MAJORINJURIESPASSENGER+a.MINORINJURIESPASSENGER+a.UNKNOWNINJURIESPASSENGER
                                                                    +a.MAJORINJURIES_BICYCLIST + a.MINORINJURIES_BICYCLIST + a.UNKNOWNINJURIES_BICYCLIST)
                        THEN (a.MAJORINJURIES_PEDESTRIAN+ a.MINORINJURIES_PEDESTRIAN + a.UNKNOWNINJURIES_PEDESTRIAN
                                                                    +a.MAJORINJURIES_DRIVER+a.MINORINJURIES_DRIVER+a.UNKNOWNINJURIES_DRIVER+a.MAJORINJURIESPASSENGER+a.MINORINJURIESPASSENGER+a.UNKNOWNINJURIESPASSENGER
                                                                    +a.MAJORINJURIES_BICYCLIST + a.MINORINJURIES_BICYCLIST + a.UNKNOWNINJURIES_BICYCLIST)
                        ELSE b.TOTAL_INJURIES end as TOTAL_INJURIES 

                ,CASE WHEN b.CRIMEID IS NULL OR b.TOTAL_MAJOR_INJURIES < (a.MAJORINJURIES_PEDESTRIAN+
                                                                    +a.MAJORINJURIES_DRIVER+a.MAJORINJURIESPASSENGER
                                                                    +a.MAJORINJURIES_BICYCLIST)
                        THEN (a.MAJORINJURIES_PEDESTRIAN+a.MAJORINJURIES_DRIVER+a.MAJORINJURIESPASSENGER+a.MAJORINJURIES_BICYCLIST)
                        ELSE b.TOTAL_MAJOR_INJURIES end as TOTAL_MAJOR_INJURIES 

                ,CASE WHEN b.CRIMEID IS NULL OR b.TOTAL_MINOR_INJURIES < (a.MINORINJURIES_PEDESTRIAN + a.UNKNOWNINJURIES_PEDESTRIAN
                                                                    +a.MINORINJURIES_DRIVER+a.UNKNOWNINJURIES_DRIVER+a.MINORINJURIESPASSENGER+a.UNKNOWNINJURIESPASSENGER
                                                                    +a.MINORINJURIES_BICYCLIST + a.UNKNOWNINJURIES_BICYCLIST)
                    THEN (a.MINORINJURIES_PEDESTRIAN + a.UNKNOWNINJURIES_PEDESTRIAN
                                                                    +a.MINORINJURIES_DRIVER+a.UNKNOWNINJURIES_DRIVER+a.MINORINJURIESPASSENGER+a.UNKNOWNINJURIESPASSENGER
                                                                    +a.MINORINJURIES_BICYCLIST + a.UNKNOWNINJURIES_BICYCLIST)
                    ELSE b.TOTAL_MINOR_INJURIES end as TOTAL_MINOR_INJURIES     

                ,CASE WHEN b.CRIMEID IS NULL OR b.BICYCLE_FATALITIES < a.FATAL_BICYCLIST
                    THEN a.FATAL_BICYCLIST 
                    ELSE b.BICYCLE_FATALITIES END AS BICYCLE_FATALITIES
                ,CASE WHEN b.CRIMEID IS NULL OR b.PEDESTRIAN_FATALITIES < a.FATAL_PEDESTRIAN
                    THEN a.FATAL_PEDESTRIAN 
                    ELSE b.PEDESTRIAN_FATALITIES END AS PEDESTRIAN_FATALITIES
                ,CASE WHEN b.CRIMEID IS NULL OR b.VEHICLE_FATALITIES < (a.FATAL_DRIVER+a.FATALPASSENGER)
                    THEN (a.FATAL_DRIVER+a.FATALPASSENGER) 
                    ELSE b.VEHICLE_FATALITIES END AS VEHICLE_FATALITIES
                ,CASE WHEN b.CRIMEID IS NULL or b.DRIVERS_IMPAIRED < a.DRIVERSIMPAIRED THEN a.DRIVERSIMPAIRED ELSE b.DRIVERS_IMPAIRED END AS DRIVERS_IMPAIRED 
                ,CASE WHEN b.CRIMEID IS NULL or b.DRIVERS_SPEEDING < a.SPEEDING_INVOLVED THEN a.SPEEDING_INVOLVED ELSE b.DRIVERS_SPEEDING END AS DRIVERS_SPEEDING 

                ,CASE WHEN b.CRIMEID IS NULL or b.TOTAL_VEHICLES < a.TOTAL_VEHICLES THEN a.TOTAL_VEHICLES ELSE b.TOTAL_VEHICLES END AS TOTAL_VEHICLES 
                ,CASE WHEN b.CRIMEID IS NULL or b.TOTAL_BICYCLISTS < a.TOTAL_BICYCLES THEN a.TOTAL_BICYCLES ELSE b.TOTAL_BICYCLISTS END AS TOTAL_BICYCLISTS 
                ,CASE WHEN b.CRIMEID IS NULL or b.TOTAL_PEDESTRIANS < a.TOTAL_PEDESTRIANS THEN a.TOTAL_PEDESTRIANS ELSE b.TOTAL_PEDESTRIANS END AS TOTAL_PEDESTRIANS 
                ,b.DRIVERS_OVER_65
                ,b.DRIVERS_UNDER_25
                ,b.PEDS_OVER_65
                ,b.PEDS_UNDER_12
                ,b.BIKERS_OVER_65
                ,b.BIKERS_UNDER_18
                ,b.OOS_VEHICLES
                ,b.NUM_CARS
                ,b.NUM_SUVS_OR_TRUCKS
                ,b.DRIVER_TICKETS
                ,b.BICYCLE_TICKETS
                ,b.PED_TICKETS
                ,b.PERSONTYPE_ARRAY
                ,b.INVEHICLETYPE_ARRAY
                ,b.LICENSEPLATESTATE_ARRAY
                ,a.INTAPPROACHDIRECTION
                ,a.LOCATIONERROR 
                ,a.LASTUPDATEDATE
                ,a.BLOCKKEY
                ,a.SUBBLOCKKEY
                ,a.geography

        FROM crashes_raw a
        LEFT JOIN crash_details_agg b on a.CRIMEID = b.CRIMEID
        WHERE date_part('year', a.fromdate) >=2015
    )

    SELECT DISTINCT a.*
         ,b.aadt
            ,b.totaltravellanes
            ,b.totalcrosssectionwidth
            ,b.totalparkinglanes
            ,b.doubleyellow_line
            ,b.summarydirection
            ,case 
                when b.sidewalk_ib_pavtype is not null and b.sidewalk_ob_pavtype is not null then 2
                when b.sidewalk_ib_pavtype is not null or b.sidewalk_ob_pavtype is not null then 1
                else 0 end as Num_Sides_W_Sidewalks
            ,coalesce(b.sidewalk_ib_width, b.sidewalk_ob_width) as sidewalk_width
            ,coalesce(b.speedlimits_ib, b.speedlimits_ob) as speed_limit
            ,b.dcfunctionalclass
            ,b.nhstype
            ,b.routename 
            ,b.streetname
            ,b.streettype
            ,case dcfunctionalclass
                when '11.0' then 'Interstate'
                when '12.0' then 'Other Freeway and Expressway'
                when '14.0' then 'Principal Arterial'
                when '16.0' then 'Minor Arterial'
                when '17.0' then 'Collector'
                when '19.0' then 'Local'
                end as dcfunctionalclass_desc
            FROM crashes_join a
            LEFT JOIN source_data.roadway_subblocks b ON a.subblockkey = b.subblockkey
    
   