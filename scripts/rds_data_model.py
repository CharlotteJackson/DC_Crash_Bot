from connect_to_rds import create_postgres_engine
import sqlalchemy

def generate_table(engine, target_schema:str, target_table:str,mode:str):

    schema_query = """
        CREATE SCHEMA IF NOT EXISTS {0};
        GRANT ALL PRIVILEGES ON SCHEMA {0} TO PUBLIC;
    """.format(target_schema)

    drop_table_query = """
        DROP TABLE IF EXISTS {}.{};
    """.format(target_schema, target_table)

    create_table_query = """
        CREATE TABLE IF NOT EXISTS {0}.{1} (
            {2}
        );
        GRANT ALL PRIVILEGES ON {0}.{1} TO PUBLIC;
    """.format(target_schema, target_table, get_table_definition(target_table))

    truncate_table_query = """
        TRUNCATE {}.{};
    """.format(target_schema, target_table)

    # always run the create schema query
    engine.execute(schema_query)
    # depending on mode, either drop or truncate target table
    if mode.lower()=='replace':
        engine.execute(drop_table_query)
    if mode.lower()=='truncate':
        engine.execute(truncate_table_query)
    # always run the create table query 
    engine.execute(create_table_query)
    
def correct_geo(engine, target_schema:str, target_table:str,mode:str):

    # check whether the target table has a geography field
    check_geo_field_query = """
    SELECT CASE WHEN EXISTS
	(SELECT 1 from information_schema.columns WHERE table_schema = '{}' AND table_name = '{}' AND column_name = 'geography') 
	THEN 1 ELSE 0 END
    """.format(target_schema, target_table)

    create_geo_index_query = """
        DROP INDEX IF EXISTS {0};
        CREATE INDEX {0}
        ON {1}.{2}
        USING GIST (geography);
    """.format(target_table+'_index',target_schema, target_table)

    update_geography_query = """
    UPDATE {}.{}  SET geography=ST_Force2D(geography::geometry)::geography
    """.format(target_schema, target_table)

    # check to see if table has a geography field, if yes, make sure it's the right format and create an index
    geo_field_exists = engine.execute(check_geo_field_query).fetchone()[0]
    if geo_field_exists==1:
        engine.execute(update_geography_query)
        engine.execute(create_geo_index_query)

def get_table_definition(target_table:str):

    data_model_dict = {
        'acs_2019_by_tract': """
                NAME VARCHAR NULL
                ,total_pop NUMERIC NULL
                ,total_households NUMERIC NULL
                ,total_households_w_no_vehicle NUMERIC NULL
                ,total_households_w_1_vehicles NUMERIC NULL
                ,total_households_w_2_vehicles NUMERIC NULL
                ,total_households_w_3_vehicles NUMERIC NULL
                ,total_households_w_4plus_vehicles NUMERIC NULL
                ,state VARCHAR NULL
                ,county VARCHAR NULL
                ,tract VARCHAR NULL
        """
        ,'acs_housing_2011_2015':"""
            OBJECTID VARCHAR PRIMARY KEY
            ,COMP_PLAN_AREA VARCHAR NULL
            ,HOUSING_TOTAL_UNITS NUMERIC NULL
            ,HOUSING_OCCUPIED_UNITS NUMERIC NULL
            ,HOUSING_VACANT_UNITS NUMERIC NULL
            ,UNITS_STRUCT_TOT_HU NUMERIC NULL
            ,UNIT_STRUCT_TOT_HU_1_DETACH NUMERIC NULL
            ,UNIT_STRUCT_TOT_HU_1_ATTACH NUMERIC NULL
            ,UNIT_STRUCT_TOT_HU_2 NUMERIC NULL
            ,UNIT_STRUCT_TOT_HU_3_OR_4 NUMERIC NULL
            ,UNIT_STRUCT_TOT_HU_5_TO_9 NUMERIC NULL
            ,UNIT_STRUCT_TOT_HU_10_TO_19 NUMERIC NULL
            ,UNIT_STRUCT_TOT_HU_20_MORE NUMERIC NULL
            ,UNIT_STRUCT_TOT_HU_MOBILE NUMERIC NULL
            ,UNIT_STRUCT_TOT_HU_REC_VEHIC NUMERIC NULL
            ,YR_STRUCT_BLT_TOT_HU NUMERIC NULL
            ,YR_STRUC_BLT_TOT_HU_2014_MORE NUMERIC NULL
            ,YR_STRUC_BLT_TOT_HU_2010_2013 NUMERIC NULL
            ,YR_STRUC_BLT_TOT_HU_2000_2009 NUMERIC NULL
            ,YR_STRUC_BLT_TOT_HU_1990_1999 NUMERIC NULL
            ,YR_STRUC_BLT_TOT_HU_1980_1989 NUMERIC NULL
            ,YR_STRUC_BLT_TOT_HU_1970_1979 NUMERIC NULL
            ,YR_STRUC_BLT_TOT_HU_1960_1969 NUMERIC NULL
            ,YR_STRUC_BLT_TOT_HU_1950_1959 NUMERIC NULL
            ,YR_STRUC_BLT_TOT_HU_1940_1949 NUMERIC NULL
            ,YR_STRUC_BLT_TOT_HU_1939_LESS NUMERIC NULL
            ,ROOMS_TOT_HU NUMERIC NULL
            ,ROOMS_TOT_HU_1_ROOM NUMERIC NULL
            ,ROOMS_TOT_HU_2_ROOM NUMERIC NULL
            ,ROOMS_TOT_HU_3_ROOM NUMERIC NULL
            ,ROOMS_TOT_HU_4_ROOM NUMERIC NULL
            ,ROOMS_TOT_HU_5_ROOM NUMERIC NULL
            ,ROOMS_TOT_HU_6_ROOM NUMERIC NULL
            ,ROOMS_TOT_HU_7_ROOM NUMERIC NULL
            ,ROOMS_TOT_HU_8_ROOM NUMERIC NULL
            ,ROOMS_TOT_HU_9_MORE_ROOM NUMERIC NULL
            ,BDROOM_TOT_HU NUMERIC NULL
            ,BDROOM_TOT_HU_NO_BDROOM NUMERIC NULL
            ,BDROOM_TOT_HU_1_BDROOM NUMERIC NULL
            ,BDROOM_TOT_HU_2_BDROOM NUMERIC NULL
            ,BDROOM_TOT_HU_3_BDROOM NUMERIC NULL
            ,BDROOM_TOT_HU_4_BDROOM NUMERIC NULL
            ,BDROOM_TOT_HU_5_MORE_BDROOM NUMERIC NULL
            ,TENURE_OCCUP_HU NUMERIC NULL
            ,TENURE_OCCUP_HU_OWNER NUMERIC NULL
            ,TENURE_OCCUP_HU_RENTER NUMERIC NULL
            ,TENURE_AVG_HH_SZ_OWNER_OCCUP NUMERIC NULL
            ,TENURE_AVG_HH_SZ_RENTER_OCCUP NUMERIC NULL
            ,YR_HH_MOVED_IN_UNIT NUMERIC NULL
            ,YR_HH_MOVED_IN_2015_OR_MORE NUMERIC NULL
            ,YR_HH_MOVED_IN_2010_TO_2014 NUMERIC NULL
            ,YR_HH_MOVED_IN_2000_TO_2009 NUMERIC NULL
            ,YR_HH_MOVED_IN_1990_TO_1999 NUMERIC NULL
            ,YR_HH_MOVED_IN_1980_TO_1989 NUMERIC NULL
            ,YR_HH_MOVED_IN_1979_OR_LESS NUMERIC NULL
            ,VEHICLE_AVAIL_OCCUP_HU NUMERIC NULL
            ,VEHICLE_AVAIL_OCCUP_HU_NONE NUMERIC NULL
            ,VEHICLE_AVAIL_OCCUP_HU_1 NUMERIC NULL
            ,VEHICLE_AVAIL_OCCUP_HU_2 NUMERIC NULL
            ,VEHICLE_AVAIL_OCCUP_HU_3_MORE NUMERIC NULL
            ,HEAT_FUEL_OCCUP_HU NUMERIC NULL
            ,HEAT_FUEL_OCCUP_HU_UTILGAS NUMERIC NULL
            ,HEAT_FUEL_OCCUP_HU_TANKGAS NUMERIC NULL
            ,HEAT_FUEL_OCCUP_HU_ELECTRIC NUMERIC NULL
            ,HEAT_FUEL_OCCUP_HU_OIL NUMERIC NULL
            ,HEAT_FUEL_OCCUP_HU_COAL NUMERIC NULL
            ,HEAT_FUEL_OCCUP_HU_WOOD NUMERIC NULL
            ,HEAT_FUEL_OCCUP_HU_SOLAR NUMERIC NULL
            ,HEAT_FUEL_OCCUP_HU_OTHER NUMERIC NULL
            ,HEAT_FUEL_OCCUP_HU_NONE NUMERIC NULL
            ,SELECT_OTHER_OCCUP_HU NUMERIC NULL
            ,OTHER_OCCUP_HU_NO_PLUMBING NUMERIC NULL
            ,OTHER_OCCUP_HU_NO_KITCHEN NUMERIC NULL
            ,OTHER_OCCUP_HU_NO_PHONE NUMERIC NULL
            ,OCCUP_ROOM_OCCUP_HU NUMERIC NULL
            ,OCPANT_ROOM_OCCUP_HU_1_LESS NUMERIC NULL
            ,OCPANT_ROOM_OCCUP_HU_1_TO_1_5 NUMERIC NULL
            ,OCPANT_ROOM_OC_HU_1_5_TO_MORE NUMERIC NULL
            ,VALUE_OWNER_OCCUP NUMERIC NULL
            ,VALUE_OWNER_OCCUP_LESS_50 NUMERIC NULL
            ,VALUE_OWNER_OCCUP_50_TO_99999 NUMERIC NULL
            ,VALUE_OWNER_OCC_100_TO_149999 NUMERIC NULL
            ,VALUE_OWNER_OCC_150_TO_199999 NUMERIC NULL
            ,VALUE_OWNER_OCC_200_TO_299999 NUMERIC NULL
            ,VALUE_OWNER_OCC_300_TO_499999 NUMERIC NULL
            ,VALUE_OWNER_OCC_500_TO_999999 NUMERIC NULL
            ,VALUE_OWNER_OCCUP_1M_MORE NUMERIC NULL
            ,MORTGE_OWNER_OCCUP NUMERIC NULL
            ,MORTGE_OWNER_OCCUP_MORTGE NUMERIC NULL
            ,MORTGE_OWNER_OCCUP_NO_MORTGE NUMERIC NULL
            ,SMOC_HU_MORTGE NUMERIC NULL
            ,SMOC_HU_MORTGE_500_LESS NUMERIC NULL
            ,SMOC_HU_MORTGE_500_TO_999 NUMERIC NULL
            ,SMOC_HU_MORTGE_1000_TO_1499 NUMERIC NULL
            ,SMOC_HU_MORTGE_1500_TO_1999 NUMERIC NULL
            ,SMOC_HU_MORTGE_2000_TO_2499 NUMERIC NULL
            ,SMOC_HU_MORTGE_2500_TO_2999 NUMERIC NULL
            ,SMOC_HU_MORTGE_3000_MORE NUMERIC NULL
            ,SMOC_HU_NO_MORTGE NUMERIC NULL
            ,SMOC_HU_NO_MORTGE_250_LESS NUMERIC NULL
            ,SMOC_HU_NO_MORTGE_250_TO_399 NUMERIC NULL
            ,SMOC_HU_NO_MORTGE_400_TO_599 NUMERIC NULL
            ,SMOC_HU_NO_MORTGE_600_TO_799 NUMERIC NULL
            ,SMOC_HU_NO_MORTGE_800_TO_999 NUMERIC NULL
            ,SMOC_HU_NO_MORTGE_1000_MORE NUMERIC NULL
            ,SMOCAPI_HU_MORTGE NUMERIC NULL
            ,SMOCAPI_HU_MORTGE_20P_LESS NUMERIC NULL
            ,SMOCAPI_HU_MORTGE_20_TO_24_9P NUMERIC NULL
            ,SMOCAPI_HU_MORTGE_25_TO_29_9P NUMERIC NULL
            ,SMOCAPI_HU_MORTGE_30_TO_34_9P NUMERIC NULL
            ,SMOCAPI_HU_MORTGE_35P_MORE NUMERIC NULL
            ,SMOCAPI_HU_MORTGE_NOT_COMPUT NUMERIC NULL
            ,SMOCAPIT_HU_NO_MORTGE NUMERIC NULL
            ,SMOCAPI_HU_NO_MORTGE_10P_LESS NUMERIC NULL
            ,SMOCAPI_HU_NO_MOR_10_TO_14_9P NUMERIC NULL
            ,SMOCAPI_HU_NO_MOR_15_TO_19_9P NUMERIC NULL
            ,SMOCAPI_HU_NO_MOR_20_TO_24_9P NUMERIC NULL
            ,SMOCAPI_HU_NO_MOR_25_TO_29_9P NUMERIC NULL
            ,SMOCAPI_HU_NO_MOR_30_TO_34_9P NUMERIC NULL
            ,SMOCAPI_HU_NO_MOR_35P_MORE NUMERIC NULL
            ,SMOCAPI_HU_NO_MOR_NOT_COMPUT NUMERIC NULL
            ,GROSS_RENT_OCCUP_HU NUMERIC NULL
            ,GROSS_RENT_OCCUP_HU_500_LESS NUMERIC NULL
            ,GROSS_RENT_OC_HU_500_TO_999 NUMERIC NULL
            ,GROSS_RENT_OC_HU_1000_TO_1499 NUMERIC NULL
            ,GROSS_RENT_OC_HU_1500_TO_1999 NUMERIC NULL
            ,GROSS_RENT_OC_HU_2000_TO_2499 NUMERIC NULL
            ,GROSS_RENT_OC_HU_2500_2999 NUMERIC NULL
            ,GROSS_RENT_OC_HU_3000_MORE NUMERIC NULL
            ,GROSS_RENT_OCCUP_HU_NO_RENT NUMERIC NULL
            ,GRAPI_OCCUP_HU_PAY_RENT NUMERIC NULL
            ,GRAPI_OC_PAY_RENT_15P_LESS NUMERIC NULL
            ,GRAPI_OC_PAY_RENT_15_TO_19_9P NUMERIC NULL
            ,GRAPI_OC_PAY_RENT_20_TO_24_9P NUMERIC NULL
            ,GRAPI_OC_PAY_RENT_25_TO_29_9P NUMERIC NULL
            ,GRAPI_OC_PAY_RENT_30_TO_34_9P NUMERIC NULL
            ,GRAPI_OC_PAY_RENT_35_MORE NUMERIC NULL
            ,GRAPI_OCCUP_HU_NOT_COMPUT NUMERIC NULL
            ,geography geography null
        """
        ,'address_points':"""
            OBJECTID_12 VARCHAR PRIMARY KEY
            ,OBJECTID VARCHAR NULL
            ,SITE_ADDRESS_PK VARCHAR NULL
            ,ADDRESS_ID VARCHAR NULL
            ,ROADWAYSEGID VARCHAR NULL
            ,STATUS VARCHAR NULL
            ,SSL VARCHAR NULL
            ,TYPE_ VARCHAR NULL
            ,ENTRANCETYPE VARCHAR NULL
            ,ADDRNUM VARCHAR NULL
            ,ADDRNUMSUFFIX VARCHAR NULL
            ,STNAME VARCHAR NULL
            ,STREET_TYPE VARCHAR NULL
            ,QUADRANT VARCHAR NULL
            ,CITY VARCHAR NULL
            ,STATE VARCHAR NULL
            ,FULLADDRESS VARCHAR NULL
            ,SQUARE VARCHAR NULL
            ,SUFFIX VARCHAR NULL
            ,LOT VARCHAR NULL
            ,NATIONALGRID VARCHAR NULL
            ,ZIPCODE4 VARCHAR NULL
            ,XCOORD VARCHAR NULL
            ,YCOORD VARCHAR NULL
            ,STATUS_ID VARCHAR NULL
            ,METADATA_ID VARCHAR NULL
            ,OBJECTID_1 VARCHAR NULL
            ,ASSESSMENT_NBHD VARCHAR NULL
            ,ASSESSMENT_SUBNBHD VARCHAR NULL
            ,CFSA_NAME VARCHAR NULL
            ,HOTSPOT VARCHAR NULL
            ,CLUSTER_ VARCHAR NULL
            ,POLDIST VARCHAR NULL
            ,ROC VARCHAR NULL
            ,PSA VARCHAR NULL
            ,SMD VARCHAR NULL
            ,CENSUS_TRACT VARCHAR NULL
            ,VOTE_PRCNCT VARCHAR NULL
            ,WARD VARCHAR NULL
            ,ZIPCODE VARCHAR NULL
            ,ANC VARCHAR NULL
            ,NEWCOMMSELECT06 VARCHAR NULL
            ,NEWCOMMCANDIDATE VARCHAR NULL
            ,CENSUS_BLOCK VARCHAR NULL
            ,CENSUS_BLOCKGROUP VARCHAR NULL
            ,FOCUS_IMPROVEMENT_AREA VARCHAR NULL
            ,SE_ANNO_CAD_DATA VARCHAR NULL
            ,LATITUDE VARCHAR NULL
            ,LONGITUDE VARCHAR NULL
            ,ACTIVE_RES_UNIT_COUNT VARCHAR NULL
            ,RES_TYPE VARCHAR NULL
            ,ACTIVE_RES_OCCUPANCY_COUNT VARCHAR NULL
            ,WARD_2002 VARCHAR NULL
            ,WARD_2012 VARCHAR NULL
            ,ANC_2002 VARCHAR NULL
            ,ANC_2012 VARCHAR NULL
            ,SMD_2002 VARCHAR NULL
            ,SMD_2012 VARCHAR NULL
            ,geography geography null
    """
    ,'anc_boundaries':"""
            OBJECTID VARCHAR NULL
            ,ANC_ID VARCHAR NULL
            ,WEB_URL VARCHAR NULL
            ,NAME VARCHAR NULL
            ,Shape_Length NUMERIC NULL
            ,Shape_Area NUMERIC NULL
            ,geography geography null
    """
    ,'all311':"""
            OBJECTID VARCHAR NULL
            ,SERVICECODE VARCHAR NULL
            ,SERVICECODEDESCRIPTION VARCHAR NULL
            ,SERVICETYPECODEDESCRIPTION VARCHAR NULL
            ,ORGANIZATIONACRONYM VARCHAR NULL
            ,SERVICECALLCOUNT VARCHAR NULL
            ,ADDDATE TIMESTAMPTZ NULL
            ,RESOLUTIONDATE TIMESTAMPTZ NULL
            ,SERVICEDUEDATE TIMESTAMPTZ NULL
            ,SERVICEORDERDATE TIMESTAMPTZ NULL
            ,INSPECTIONFLAG VARCHAR NULL
            ,INSPECTIONDATE TIMESTAMPTZ NULL
            ,INSPECTORNAME VARCHAR NULL
            ,SERVICEORDERSTATUS VARCHAR NULL
            ,STATUS_CODE VARCHAR NULL
            ,SERVICEREQUESTID VARCHAR NULL
            ,PRIORITY VARCHAR NULL
            ,STREETADDRESS VARCHAR NULL
            ,XCOORD VARCHAR NULL
            ,YCOORD VARCHAR NULL
            ,LATITUDE VARCHAR NULL
            ,LONGITUDE VARCHAR NULL
            ,CITY VARCHAR NULL
            ,STATE VARCHAR NULL
            ,ZIPCODE VARCHAR NULL
            ,MARADDRESSREPOSITORYID VARCHAR NULL
            ,WARD VARCHAR NULL
            ,DETAILS VARCHAR NULL
            ,geography geography null
        """
    ,'census_blocks':"""
            OBJECTID VARCHAR PRIMARY KEY
            ,BLKGRP VARCHAR NULL
            ,BLOCK VARCHAR NULL
            ,GEOID VARCHAR NULL
            ,GEOID10 VARCHAR NULL
            ,ALAND10 VARCHAR NULL
            ,AWATER10 VARCHAR NULL
            ,TOTAL_POP INT NULL
            ,TOTAL_POP_ONE_RACE INT NULL
            ,POP_WHITE INT NULL
            ,POP_BLACK INT NULL
            ,POP_NATIVE INT NULL
            ,POP_ASIAN INT NULL
            ,POP_PACIFIC_ISLANDER INT NULL
            ,POP_OTHER_RACE INT NULL
            ,POP_BLACK_OTHER INT NULL
            ,POP_NATIVE_OTHER INT NULL
            ,POP_ASIAN_OTHER INT NULL
            ,POP_PACIFIC_ISLANDER_OTHER INT NULL
            ,POP_HISPANIC INT NULL
            ,POP_WHITE_NON_HISPANIC INT NULL
            ,POP_NON_HISPANIC_BLACK INT NULL
            ,POP_NON_HISPANIC_NATIVE INT NULL
            ,POP_NON_HISPANIC_ASIAN INT NULL
            ,POP_NON_HISPANIC_PACIFIC_ISLANDER INT NULL
            ,POP_NON_HISPANIC_OTHER INT NULL
            ,POP_NON_HISPANIC_BLACK_OTHER INT NULL
            ,POP_NON_HISPANIC_NATIVE_OTHER INT NULL
            ,POP_NON_HISPANIC_ASIAN_OTHER INT NULL
            ,POP_NON_HISPANIC_PACIFIC_ISLANDER_OTHER INT NULL
            ,ADULT_TOTAL_POP INT NULL
            ,ADULT_POP_WHITE INT NULL
            ,ADULT_POP_BLACK INT NULL
            ,ADULT_POP_NATIVE INT NULL
            ,ADULT_POP_ASIAN INT NULL
            ,ADULT_POP_PACIFIC_ISLANDER INT NULL
            ,ADULT_POP_OTHER_RACE INT NULL
            ,ADULT_POP_BLACK_OTHER INT NULL
            ,ADULT_POP_NATIVE_OTHER INT NULL
            ,ADULT_POP_ASIAN_OTHER INT NULL
            ,ADULT_POP_PACIFIC_ISLANDER_OTHER INT NULL
            ,ADULT_POP_HISPANIC INT NULL
            ,ADULT_POP_WHITE_NON_HISPANIC INT NULL
            ,ADULT_POP_NON_HISPANIC_BLACK INT NULL
            ,ADULT_POP_NON_HISPANIC_NATIVE INT NULL
            ,ADULT_POP_NON_HISPANIC_ASIAN INT NULL
            ,ADULT_POP_NON_HISPANIC_PACIFIC_ISLANDER INT NULL
            ,ADULT_POP_NON_HISPANIC_OTHER INT NULL
            ,ADULT_POP_NON_HISPANIC_BLACK_OTHER INT NULL
            ,ADULT_POP_NON_HISPANIC_NATIVE_OTHER INT NULL
            ,ADULT_POP_NON_HISPANIC_ASIAN_OTHER INT NULL
            ,ADULT_POP_NON_HISPANIC_PACIFIC_ISLANDER_OTHER INT NULL
            ,TOTAL_HOUSING_UNITS INT NULL
            ,TOTAL_OCCUPIED_HOUSING_UNITS INT NULL
            ,TOTAL_VACANT_HOUSING_UNITS INT NULL
            ,ACRES VARCHAR NULL
            ,Shape_Length VARCHAR NULL
            ,Shape_Area VARCHAR NULL
            ,SQMILES VARCHAR NULL
            ,geography geography null
    """
    ,'census_tracts':"""
        OBJECTID VARCHAR PRIMARY KEY
        ,TRACT VARCHAR NULL
        ,GEOID VARCHAR NULL
        ,total_pop NUMERIC NULL
        ,total_pop_1_race NUMERIC NULL
        ,total_pop_1_race_white NUMERIC NULL
        ,total_pop_1_race_black NUMERIC NULL
        ,total_pop_1_race_native NUMERIC NULL
        ,total_pop_1_race_asian NUMERIC NULL
        ,total_pop_1_race_pacific_islander NUMERIC NULL
        ,total_pop_1_race_other NUMERIC NULL
        ,total_pop_2_races_black_and NUMERIC NULL
        ,total_pop_2_races_native_and NUMERIC NULL
        ,total_pop_2_races_asian_and NUMERIC NULL
        ,total_pop_2_races_pacific_islander_and NUMERIC NULL
        ,total_pop_hispanic NUMERIC NULL
        ,pop_non_hispanic_white NUMERIC NULL
        ,pop_non_hispanic_black NUMERIC NULL
        ,pop_non_hispanic_native NUMERIC NULL
        ,pop_non_hispanic_asian NUMERIC NULL
        ,pop_non_hispanic_pacific_islander NUMERIC NULL
        ,pop_non_hispanic_other NUMERIC NULL
        ,pop_non_hispanic_2_races_black_and NUMERIC NULL
        ,pop_non_hispanic_2_races_native_and NUMERIC NULL
        ,pop_non_hispanic_2_races_asian_and NUMERIC NULL
        ,pop_non_hispanic_2_races_pacific_islander_and NUMERIC NULL
        ,total_pop_over_18 NUMERIC NULL
        ,total_pop_over_18_white NUMERIC NULL
        ,total_pop_over_18_black NUMERIC NULL
        ,total_pop_over_18_native NUMERIC NULL
        ,total_pop_over_18_asian NUMERIC NULL
        ,total_pop_over_18_pacific_islander NUMERIC NULL
        ,total_pop_over_18_other NUMERIC NULL
        ,pop_over_18_black_and NUMERIC NULL
        ,pop_over_18_native_and NUMERIC NULL
        ,pop_over_18_asian_and NUMERIC NULL
        ,pop_over_18_pacific_islander_and NUMERIC NULL
        ,hispanic_pop_over_18 NUMERIC NULL
        ,non_hispanic_white_pop_over_18 NUMERIC NULL
        ,non_hispanic_black_pop_over_18 NUMERIC NULL
        ,non_hispanic_native_pop_over_18 NUMERIC NULL
        ,non_hispanic_asian_pop_over_18 NUMERIC NULL
        ,non_hispanic_pacific_islander_pop_over_18 NUMERIC NULL
        ,non_hispanic_other_race_pop_over_18 NUMERIC NULL
        ,OP000013 NUMERIC NULL
        ,OP000014 NUMERIC NULL
        ,OP000015 NUMERIC NULL
        ,OP000016 NUMERIC NULL
        ,total_housing_units NUMERIC NULL
        ,occupied_housing_units NUMERIC NULL
        ,vacant_housing_units NUMERIC NULL
        ,ACRES NUMERIC NULL
        ,SQ_MILES NUMERIC NULL
        ,Shape_Length NUMERIC NULL
        ,Shape_Area NUMERIC NULL
        ,FAGI_TOTAL_2010 NUMERIC NULL
        ,FAGI_MEDIAN_2010 NUMERIC NULL
        ,FAGI_TOTAL_2013 NUMERIC NULL
        ,FAGI_MEDIAN_2013 NUMERIC NULL
        ,FAGI_TOTAL_2011 NUMERIC NULL
        ,FAGI_MEDIAN_2011 NUMERIC NULL
        ,FAGI_TOTAL_2012 NUMERIC NULL
        ,FAGI_MEDIAN_2012 NUMERIC NULL
        ,FAGI_TOTAL_2014 NUMERIC NULL
        ,FAGI_MEDIAN_2014 NUMERIC NULL
        ,FAGI_TOTAL_2015 NUMERIC NULL
        ,FAGI_MEDIAN_2015 NUMERIC NULL
        ,geography geography
"""
    ,'charter_schools':"""
            OBJECTID VARCHAR NULL
            ,NAME VARCHAR NULL
            ,ADDRESS VARCHAR NULL
            ,DIRECTOR VARCHAR NULL
            ,PHONE VARCHAR NULL
            ,AUTHORIZER VARCHAR NULL
            ,GRADES VARCHAR NULL
            ,ENROLLMENT VARCHAR NULL
            ,GIS_ID VARCHAR NULL
            ,WEB_URL VARCHAR NULL
            ,ADDRID VARCHAR NULL
            ,X VARCHAR NULL
            ,Y VARCHAR NULL
            ,AUTHORIZAT VARCHAR NULL
            ,MYSCHOOL VARCHAR NULL
            ,SCHOOL_YEA VARCHAR NULL
            ,LEA_NAME VARCHAR NULL
            ,LEA_ID VARCHAR NULL
            ,SCHOOL_NAM VARCHAR NULL
            ,SCHOOL_ID VARCHAR NULL
            ,SCHOOLCODE VARCHAR NULL
            ,GRADES_1 VARCHAR NULL
            ,LATITUDE VARCHAR NULL
            ,LONGITUDE VARCHAR NULL
            ,FACILITY_ID VARCHAR NULL
            ,CLUSTER_ VARCHAR NULL
            ,WARD VARCHAR NULL
            ,ZIPCODE VARCHAR NULL
            ,geography geography null
    """
    ,'cityworks_service_requests':"""
            OBJECTID VARCHAR PRIMARY KEY
            ,REQUESTID VARCHAR NULL
            ,WORKORDERID VARCHAR NULL
            ,CSRNUMBER VARCHAR NULL
            ,DESCRIPTION VARCHAR NULL
            ,STATUS VARCHAR NULL
            ,REQUESTCATEGORY VARCHAR NULL
            ,INITIATEDDATE TIMESTAMPTZ NULL
            ,CLOSEDDATE VARCHAR NULL
            ,INSPECTIONDATE TIMESTAMPTZ NULL
            ,INSPECTIONCOMPLETE VARCHAR NULL
            ,SUBMITTEDTODATE TIMESTAMPTZ NULL
            ,DISPATCHEDTODATE TIMESTAMPTZ NULL
            ,CANCELEDDATE TIMESTAMPTZ NULL
            ,PRIORITY VARCHAR NULL
            ,INITIATEDBY VARCHAR NULL
            ,SUBMITTEDTO VARCHAR NULL
            ,DISPATCHEDTO VARCHAR NULL
            ,CLOSEDBY VARCHAR NULL
            ,PROJECTNAME VARCHAR NULL
            ,ISCANCELED VARCHAR NULL
            ,CANCELEDBY VARCHAR NULL
            ,ADDRESS VARCHAR NULL
            ,FISCALYEAR NUMERIC NULL
            ,WARD VARCHAR NULL
            ,QUADRANT VARCHAR NULL
            ,ZIPCODE VARCHAR NULL
            ,ANC VARCHAR NULL
            ,SMD VARCHAR NULL
            ,NEIGHBORHOODCLUSTERS VARCHAR NULL
            ,NEIGHBORHOODNAMES VARCHAR NULL
            ,BID VARCHAR NULL
            ,AWI VARCHAR NULL
            ,EDZ VARCHAR NULL
            ,NIF VARCHAR NULL
            ,HISTORICDISTRICT VARCHAR NULL
            ,ZONING VARCHAR NULL
            ,PUD VARCHAR NULL
            ,CFAR VARCHAR NULL
            ,PSA VARCHAR NULL
            ,PD VARCHAR NULL
            ,DAYSTOCLOSE NUMERIC NULL
            ,DAYSTOINSPECT NUMERIC NULL
            ,UPDATEDATE TIMESTAMPTZ NULL
            ,XCOORD NUMERIC NULL
            ,YCOORD NUMERIC NULL
            ,ONSEGX NUMERIC NULL
            ,ONSEGY NUMERIC NULL
            ,LONGITUDE NUMERIC NULL
            ,LATITUDE NUMERIC NULL
            ,SQUARE VARCHAR NULL
            ,SUFFIX VARCHAR NULL
            ,LOT VARCHAR NULL
            ,geography geography null
    """
    ,'cityworks_work_orders':"""
            OBJECTID VARCHAR PRIMARY KEY
            ,WORKORDERID VARCHAR NULL
            ,PROJECTID VARCHAR NULL
            ,DESCRIPTION VARCHAR NULL
            ,STATUS VARCHAR NULL
            ,INITIATEDDATE TIMESTAMPTZ NULL
            ,WORKORDERCLOSEDDATE TIMESTAMPTZ NULL
            ,ACTUALSTARTDATE TIMESTAMPTZ NULL
            ,ACTUALFINISHDATE TIMESTAMPTZ NULL
            ,PROJECTNAME VARCHAR NULL
            ,PRIORITY VARCHAR NULL
            ,SOURCEWORKORDERID VARCHAR NULL
            ,CYCLETYPE VARCHAR NULL
            ,SCHEDULEDATE TIMESTAMPTZ NULL
            ,WORKORDERCATEGORY VARCHAR NULL
            ,UNATTACHED VARCHAR NULL
            ,WORKORDERCOST NUMERIC NULL
            ,WORKORDERLABORCOST NUMERIC NULL
            ,WORKORDERMATERIALCOST NUMERIC NULL
            ,WORKORDEREQUIPMENTCOST NUMERIC NULL
            ,SUBMITTEDTO VARCHAR NULL
            ,SUBMITTEDTODATE TIMESTAMPTZ NULL
            ,WORKCOMPLETEDBY VARCHAR NULL
            ,WORKORDERCLOSEDBY VARCHAR NULL
            ,ISCANCELED VARCHAR NULL
            ,CANCELEDBY VARCHAR NULL
            ,CANCELEDDATE TIMESTAMPTZ NULL
            ,ASSETGROUP VARCHAR NULL
            ,SUPERVISOR VARCHAR NULL
            ,REQUESTEDBY VARCHAR NULL
            ,INITIATEDBY VARCHAR NULL
            ,ADDRESS VARCHAR NULL
            ,FISCALYEAR INT NULL
            ,WARD VARCHAR NULL
            ,QUADRANT VARCHAR NULL
            ,ZIPCODE VARCHAR NULL
            ,ANC VARCHAR NULL
            ,SMD VARCHAR NULL
            ,NEIGHBORHOODCLUSTERS VARCHAR NULL
            ,NEIGHBORHOODNAMES VARCHAR NULL
            ,BID VARCHAR NULL
            ,AWI VARCHAR NULL
            ,EDZ VARCHAR NULL
            ,NIF VARCHAR NULL
            ,HISTORICDISTRICT VARCHAR NULL
            ,ZONING VARCHAR NULL
            ,PUD VARCHAR NULL
            ,CFAR VARCHAR NULL
            ,PSA VARCHAR NULL
            ,PD VARCHAR NULL
            ,DAYSTOCLOSE NUMERIC NULL
            ,UPDATEDATE TIMESTAMPTZ NULL
            ,XCOORD NUMERIC NULL
            ,YCOORD NUMERIC NULL
            ,ONSEGX NUMERIC NULL
            ,ONSEGY NUMERIC NULL
            ,LONGITUDE NUMERIC NULL
            ,LATITUDE NUMERIC NULL
            ,GlobalID VARCHAR NULL
            ,SHAPE geometry null
            ,geography geography null
    """
    ,'comp_plan_areas':"""
            OBJECTID_1 VARCHAR PRIMARY KEY
            ,OBJECTID VARCHAR NULL   
            ,NAME VARCHAR NULL
            ,AREA NUMERIC NULL
            ,SHAPE_Length NUMERIC NULL
            ,SHAPE_Area NUMERIC NULL
            ,geography geography null
    """
    ,'crash_details':"""
            OBJECTID VARCHAR PRIMARY KEY
            ,CRIMEID VARCHAR NULL
            ,CCN VARCHAR NULL
            ,PERSONID VARCHAR NULL
            ,PERSONTYPE VARCHAR NULL
            ,AGE NUMERIC NULL
            ,FATAL VARCHAR NULL
            ,MAJORINJURY VARCHAR NULL
            ,MINORINJURY VARCHAR NULL
            ,VEHICLEID VARCHAR NULL
            ,INVEHICLETYPE VARCHAR NULL
            ,TICKETISSUED VARCHAR NULL
            ,LICENSEPLATESTATE VARCHAR NULL
            ,IMPAIRED VARCHAR NULL
            ,SPEEDING VARCHAR NULL
            ,geography geography null

    """
    ,'crashes_raw':"""
            OBJECTID VARCHAR PRIMARY KEY
            ,CRIMEID VARCHAR NULL
            ,CCN VARCHAR NULL
            ,REPORTDATE TIMESTAMPTZ NULL
            ,ROUTEID VARCHAR NULL
            ,MEASURE VARCHAR NULL
            ,_OFFSET VARCHAR NULL
            ,STREETSEGID VARCHAR NULL
            ,ROADWAYSEGID VARCHAR NULL
            ,FROMDATE DATE NULL
            ,TODATE DATE NULL
            ,MARID VARCHAR NULL
            ,ADDRESS VARCHAR NULL
            ,LATITUDE VARCHAR NULL
            ,LONGITUDE VARCHAR NULL
            ,XCOORD VARCHAR NULL
            ,YCOORD VARCHAR NULL
            ,WARD VARCHAR NULL
            ,EVENTID VARCHAR NULL
            ,MAR_ADDRESS VARCHAR NULL
            ,MAR_SCORE VARCHAR NULL
            ,MAJORINJURIES_BICYCLIST INT NULL
            ,MINORINJURIES_BICYCLIST INT NULL
            ,UNKNOWNINJURIES_BICYCLIST INT NULL
            ,FATAL_BICYCLIST INT NULL
            ,MAJORINJURIES_DRIVER INT NULL
            ,MINORINJURIES_DRIVER INT NULL
            ,UNKNOWNINJURIES_DRIVER INT NULL
            ,FATAL_DRIVER INT NULL
            ,MAJORINJURIES_PEDESTRIAN INT NULL
            ,MINORINJURIES_PEDESTRIAN INT NULL
            ,UNKNOWNINJURIES_PEDESTRIAN INT NULL
            ,FATAL_PEDESTRIAN INT NULL
            ,TOTAL_VEHICLES INT NULL
            ,TOTAL_BICYCLES INT NULL
            ,TOTAL_PEDESTRIANS INT NULL
            ,PEDESTRIANSIMPAIRED INT NULL
            ,BICYCLISTSIMPAIRED INT NULL
            ,DRIVERSIMPAIRED INT NULL
            ,TOTAL_TAXIS INT NULL
            ,TOTAL_GOVERNMENT INT NULL
            ,SPEEDING_INVOLVED INT NULL
            ,NEARESTINTROUTEID VARCHAR NULL
            ,NEARESTINTSTREETNAME VARCHAR NULL
            ,OFFINTERSECTION VARCHAR NULL
            ,INTAPPROACHDIRECTION VARCHAR NULL
            ,LOCATIONERROR VARCHAR NULL
            ,LASTUPDATEDATE TIMESTAMPTZ NULL
            ,MPDLATITUDE VARCHAR NULL
            ,MPDLONGITUDE VARCHAR NULL
            ,MPDGEOX VARCHAR NULL
            ,MPDGEOY VARCHAR NULL
            ,BLOCKKEY VARCHAR NULL
            ,SUBBLOCKKEY VARCHAR NULL
            ,FATALPASSENGER INT NULL
            ,MAJORINJURIESPASSENGER INT NULL
            ,MINORINJURIESPASSENGER INT NULL
            ,UNKNOWNINJURIESPASSENGER INT NULL
            ,geography geography null
            """
        ,'dc_metro_stations':"""
                OBJECTID VARCHAR PRIMARY KEY
                ,GIS_ID VARCHAR NULL
                ,NAME VARCHAR NULL
                ,WEB_URL VARCHAR NULL
                ,EXIT_TO_STREET VARCHAR NULL
                ,FEATURECOD VARCHAR NULL
                ,DESCRIPTION VARCHAR NULL
                ,CAPTUREYEAR TIMESTAMPTZ NULL
                ,LINE VARCHAR NULL
                ,ADDRESS_ID VARCHAR NULL
                ,ADDRESS VARCHAR NULL
                ,geography geography null
        """
        ,'intersection_points':"""
            OBJECTID VARCHAR PRIMARY KEY
            ,MARID VARCHAR NULL
            ,INTERSECTIONID VARCHAR NULL
            ,STREET1ID VARCHAR NULL
            ,STREET2ID VARCHAR NULL
            ,ST1NAME VARCHAR NULL
            ,ST1TYPE VARCHAR NULL
            ,ST1QUAD VARCHAR NULL
            ,FULLSTREET1DISPLAY VARCHAR NULL
            ,ST2NAME VARCHAR NULL
            ,ST2TYPE VARCHAR NULL
            ,ST2QUAD VARCHAR NULL
            ,FULLSTREET2DISPLAY VARCHAR NULL
            ,FULLINTERSECTION VARCHAR NULL
            ,REFX VARCHAR NULL
            ,REFY VARCHAR NULL
            ,NATIONALGRID VARCHAR NULL
            ,STREET1SEGID VARCHAR NULL
            ,STREET2SEGID VARCHAR NULL
            ,NODEID VARCHAR NULL
            ,INTERSECTION_TYPE VARCHAR NULL
            ,SOURCE VARCHAR NULL
            ,LATITUDE NUMERIC NULL
            ,LONGITUDE NUMERIC NULL
            ,geography geography
    """
    ,'metro_stations_daily_ridership':"""
            OBJECTID VARCHAR PRIMARY KEY
            ,Station VARCHAR NULL
            ,StationNumber VARCHAR NULL
            ,y1977 NUMERIC NULL
            ,y1978 NUMERIC NULL
            ,y1979 NUMERIC NULL
            ,y1980 NUMERIC NULL
            ,y1981 NUMERIC NULL
            ,y1982 NUMERIC NULL
            ,y1984 NUMERIC NULL
            ,y1985 NUMERIC NULL
            ,y1986 NUMERIC NULL
            ,y1987 NUMERIC NULL
            ,y1988 NUMERIC NULL
            ,y1989 NUMERIC NULL
            ,y1990 NUMERIC NULL
            ,y1991 NUMERIC NULL
            ,y1992 NUMERIC NULL
            ,y1993 NUMERIC NULL
            ,y1994 NUMERIC NULL
            ,y1995 NUMERIC NULL
            ,y1996 NUMERIC NULL
            ,y1997 NUMERIC NULL
            ,y1998 NUMERIC NULL
            ,y1999 NUMERIC NULL
            ,y2000 NUMERIC NULL
            ,y2001 NUMERIC NULL
            ,y2002 NUMERIC NULL
            ,y2003 NUMERIC NULL
            ,y2004 NUMERIC NULL
            ,y2005 NUMERIC NULL
            ,y2006 NUMERIC NULL
            ,y2007 NUMERIC NULL
            ,y2008 NUMERIC NULL
            ,y2009 NUMERIC NULL
            ,y2010 NUMERIC NULL
            ,y2011 NUMERIC NULL
            ,y2012 NUMERIC NULL
            ,y2013 NUMERIC NULL
            ,y2014 NUMERIC NULL
            ,y2015 NUMERIC NULL
            ,y2016 NUMERIC NULL
            ,y2017 NUMERIC NULL
            ,y2018 NUMERIC NULL
            ,XCOORD VARCHAR NULL
            ,YCOORD VARCHAR NULL
            ,geography geography null
    """
    ,'moving_violations': """
            OBJECTID VARCHAR NULL
            ,LOCATION VARCHAR NULL
            ,XCOORD NUMERIC NULL
            ,YCOORD NUMERIC NULL
            ,ISSUE_DATE TIMESTAMPTZ NULL
            ,ISSUE_TIME VARCHAR NULL
            ,ISSUING_AGENCY_CODE VARCHAR NULL
            ,ISSUING_AGENCY_NAME VARCHAR NULL
            ,ISSUING_AGENCY_SHORT VARCHAR NULL
            ,VIOLATION_CODE VARCHAR NULL
            ,VIOLATION_PROCESS_DESC VARCHAR NULL
            ,PLATE_STATE VARCHAR NULL
            ,ACCIDENT_INDICATOR VARCHAR NULL
            ,DISPOSITION_CODE VARCHAR NULL
            ,DISPOSITION_TYPE VARCHAR NULL
            ,DISPOSITION_DATE TIMESTAMPTZ NULL
            ,FINE_AMOUNT NUMERIC NULL
            ,TOTAL_PAID NUMERIC NULL
            ,PENALTY_1 NUMERIC NULL
            ,PENALTY_2 NUMERIC NULL
            ,PENALTY_3 NUMERIC NULL
            ,PENALTY_4 NUMERIC NULL
            ,PENALTY_5 NUMERIC NULL
            ,RP_MULT_OWNER_NO VARCHAR NULL
            ,BODY_STYLE VARCHAR NULL
            ,LATITUDE NUMERIC NULL
            ,LONGITUDE NUMERIC NULL
            ,MAR_ID VARCHAR NULL
            ,GIS_LAST_MOD_DTTM TIMESTAMPTZ NULL
            ,DRV_LIC_STATE VARCHAR NULL
            ,DOB_YEAR INT NULL
            ,VEH_YEAR INT NULL
            ,VEH_MAKE VARCHAR NULL
            ,geography geography null
        """  
    ,'national_parks':"""
            OBJECTID VARCHAR NULL
            ,GIS_ID VARCHAR NULL
            ,NAME VARCHAR NULL
            ,ALPHA_CODE VARCHAR NULL
            ,PARK_CODE VARCHAR NULL
            ,LOCATION VARCHAR NULL
            ,AQUIRED VARCHAR NULL
            ,TRANS_DC VARCHAR NULL
            ,SOURCE VARCHAR NULL
            ,RESERVE VARCHAR NULL
            ,LABEL VARCHAR NULL
            ,Shape_Length VARCHAR NULL
            ,Shape_Area VARCHAR NULL
            ,SQUARE VARCHAR NULL
            ,SUFFIX VARCHAR NULL
            ,LOT VARCHAR NULL
            ,SSL VARCHAR NULL
            ,PAR VARCHAR NULL
            ,geography geography null
    """
    ,'neighborhood_clusters':"""
            OBJECTID VARCHAR PRIMARY KEY
            ,WEB_URL VARCHAR NULL
            ,NAME VARCHAR NULL
            ,NBH_NAMES VARCHAR NULL
            ,Shape_Length NUMERIC NULL
            ,Shape_Area NUMERIC NULL
            ,TYPE VARCHAR NULL 
            ,geography geography null
    """ 
    ,'public_schools':"""
            OBJECTID VARCHAR PRIMARY KEY
            ,NAME VARCHAR NULL
            ,ADDRESS VARCHAR NULL
            ,FACUSE VARCHAR NULL
            ,LEVEL_ VARCHAR NULL
            ,STATUS VARCHAR NULL
            ,PHONE VARCHAR NULL
            ,TOTAL_STUD NUMERIC NULL
            ,SSL VARCHAR NULL
            ,GIS_ID VARCHAR NULL
            ,WEB_URL VARCHAR NULL
            ,BLDG_NUM VARCHAR NULL
            ,SCH_PROG VARCHAR NULL
            ,CAPITALGAINS VARCHAR NULL
            ,CAPACITY VARCHAR NULL
            ,YEAR_BUILT VARCHAR NULL
            ,SQUARE_FOOTAGE NUMERIC NULL
            ,POPULATION_PLAN VARCHAR NULL
            ,LONGITUDE VARCHAR NULL
            ,LATITUDE VARCHAR NULL
            ,SCHOOL_YEA VARCHAR NULL
            ,LEA_NAME VARCHAR NULL
            ,LEA_ID VARCHAR NULL
            ,SCHOOL_NAM VARCHAR NULL
            ,SCHOOL_ID VARCHAR NULL
            ,GRADES VARCHAR NULL
            ,MAR_ID VARCHAR NULL
            ,XCOORD VARCHAR NULL
            ,YCOORD VARCHAR NULL
            ,ZIPCODE VARCHAR NULL
            ,PK3 VARCHAR NULL
            ,PK4 VARCHAR NULL
            ,UN_CE VARCHAR NULL
            ,FACILITY_ID VARCHAR NULL
            ,geography geography null
    """
    ,'pulsepoint_stream':"""
            Status_At_Scrape VARCHAR NULL
            ,Scrape_Datetime TIMESTAMPTZ NULL
            ,Incident_ID VARCHAR NULL
            ,CALL_RECEIVED_DATETIME TIMESTAMPTZ NULL
            ,CALL_Closed_DATETIME TIMESTAMPTZ NULL
            ,Latitude NUMERIC NULL
            ,Longitude NUMERIC NULL
            ,FullDisplayAddress VARCHAR NULL
            ,Incident_Type VARCHAR NULL
            ,Unit VARCHAR NULL
            ,Unit_Status_Transport INT NULL
            ,Transport_Unit_Is_AMR INT NULL
            ,Transport_Unit_Is_Non_AMR INT NULL
            ,Agency_ID VARCHAR NULL
        """ 
    ,'roadway_blocks':"""
            OBJECTID VARCHAR PRIMARY KEY
            ,ROUTEID VARCHAR NULL
            ,FROMMEASURE NUMERIC NULL
            ,TOMEASURE NUMERIC NULL
            ,ROUTENAME VARCHAR NULL
            ,ROADTYPE VARCHAR NULL
            ,BLOCKKEY VARCHAR NULL
            ,TOTALTRAVELLANES NUMERIC NULL
            ,TOTALPARKINGLANES NUMERIC NULL
            ,TOTALRAISEDBUFFERS NUMERIC NULL
            ,TOTALTRAVELLANEWIDTH NUMERIC NULL
            ,TOTALCROSSSECTIONWIDTH NUMERIC NULL
            ,TOTALPARKINGLANEWIDTH NUMERIC NULL
            ,TOTALRAISEDBUFFERWIDTH NUMERIC NULL
            ,TOTALTRAVELLANESINBOUND NUMERIC NULL
            ,TOTALTRAVELLANESOUTBOUND NUMERIC NULL
            ,TOTALTRAVELLANESBIDIRECTIONAL NUMERIC NULL
            ,TOTALTRAVELLANESREVERSIBLE NUMERIC NULL
            ,SUMMARYDIRECTION VARCHAR NULL
            ,BIKELANE_PARKINGLANE_ADJACENT VARCHAR NULL
            ,BIKELANE_THROUGHLANE_ADJACENT VARCHAR NULL
            ,BIKELANE_POCKETLANE_ADJACENT VARCHAR NULL
            ,BIKELANE_CONTRAFLOW VARCHAR NULL
            ,BIKELANE_CONVENTIONAL VARCHAR NULL
            ,BIKELANE_DUAL_PROTECTED VARCHAR NULL
            ,BIEKLANE_DUAL_BUFFERED VARCHAR NULL
            ,BIKELANE_PROTECTED VARCHAR NULL
            ,BIKELANE_BUFFERED VARCHAR NULL
            ,DOUBLEYELLOW_LINE VARCHAR NULL
            ,SECTIONFLAGS VARCHAR NULL
            ,LOC_ERROR VARCHAR NULL
            ,MIDMEASURE NUMERIC NULL
            ,AADT NUMERIC NULL
            ,AADT_YEAR NUMERIC NULL
            ,FHWAFUNCTIONALCLASS NUMERIC NULL
            ,HPMSID VARCHAR NULL
            ,HPMSSECTIONTYPE NUMERIC NULL
            ,ID VARCHAR NULL
            ,IRI NUMERIC NULL
            ,IRI_DATE TIMESTAMPTZ NULL
            ,NHSCODE NUMERIC NULL
            ,OWNERSHIP VARCHAR NULL
            ,PCI_CONDCATEGORY VARCHAR NULL
            ,PCI_LASTINSPECTED TIMESTAMPTZ NULL
            ,PCI_SCORE NUMERIC NULL
            ,QUADRANT VARCHAR NULL
            ,SIDEWALK_IB_PAVTYPE VARCHAR NULL
            ,SIDEWALK_IB_WIDTH VARCHAR NULL
            ,SIDEWALK_OB_PAVTYPE VARCHAR NULL
            ,SIDEWALK_OB_WIDTH VARCHAR NULL
            ,SPEEDLIMITS_IB NUMERIC NULL
            ,SPEEDLIMITS_IB_ALT VARCHAR NULL
            ,SPEEDLIMITS_OB NUMERIC NULL
            ,SPEEDLIMITS_OB_ALT VARCHAR NULL
            ,STREETNAME VARCHAR NULL
            ,STREETTYPE VARCHAR NULL
            ,BLOCK_NAME VARCHAR NULL
            ,ADDRESS_RANGE_HIGH NUMERIC NULL
            ,ADDRESS_RANGE_LOW NUMERIC NULL
            ,ADDRESS_RANGE_RIGHT_HIGH NUMERIC NULL
            ,ADDRESS_RANGE_LEFT_HIGH NUMERIC NULL
            ,ADDRESS_RANGE_RIGHT_LOW NUMERIC NULL
            ,MAR_ID NUMERIC NULL
            ,ADDRESS_RANGE_LEFT_LOW NUMERIC NULL
            ,BLOCKID VARCHAR NULL
            ,DCFUNCTIONALCLASS NUMERIC NULL
            ,NHSTYPE VARCHAR NULL
            ,SNOWROUTE_DDOT VARCHAR NULL
            ,SNOWROUTE_DPW VARCHAR NULL
            ,SNOWSECTION_DDOT VARCHAR NULL
            ,SNOWZONE_DDOT VARCHAR NULL
            ,SNOWZONE_DPW VARCHAR NULL
            ,LEFTTURN_CURBLANE_EXCL VARCHAR NULL
            ,LEFTTURN_CURBLANE_EXCL_LEN NUMERIC NULL
            ,RIGHTTURN_CURBLANE_EXCL VARCHAR NULL
            ,RIGHTTURN_CURBLANE_EXCL_LEN NUMERIC NULL
            ,TOTALBIKELANES NUMERIC NULL
            ,TOTALBIKELANEWIDTH NUMERIC NULL
            ,RPPDIRECTION VARCHAR NULL
            ,RPPSIDE VARCHAR NULL
            ,SLOWSTREETINFO VARCHAR NULL
            ,SHAPELEN NUMERIC NULL
            ,SHAPE VARCHAR NULL
            ,geography geography null
    """
    ,'roadway_subblocks':"""
            OBJECTID VARCHAR PRIMARY KEY
            ,ROUTEID VARCHAR NULL
            ,FROMMEASURE NUMERIC NULL
            ,TOMEASURE NUMERIC NULL
            ,ROUTENAME VARCHAR NULL
            ,ROADTYPE VARCHAR NULL
            ,SUBBLOCKKEY VARCHAR NULL
            ,TOTALTRAVELLANES NUMERIC NULL
            ,TOTALPARKINGLANES NUMERIC NULL
            ,TOTALRAISEDBUFFERS NUMERIC NULL
            ,TOTALTRAVELLANEWIDTH NUMERIC NULL
            ,TOTALCROSSSECTIONWIDTH NUMERIC NULL
            ,TOTALPARKINGLANEWIDTH NUMERIC NULL
            ,TOTALTRAVELLANESINBOUND NUMERIC NULL
            ,TOTALTRAVELLANESOUTBOUND NUMERIC NULL
            ,TOTALTRAVELLANESBIDIRECTIONAL NUMERIC NULL
            ,TOTALTRAVELLANESREVERSIBLE NUMERIC NULL
            ,SUMMARYDIRECTION VARCHAR NULL
            ,BIKELANE_PARKINGLANE_ADJACENT VARCHAR NULL
            ,BIKELANE_THROUGHLANE_ADJACENT VARCHAR NULL
            ,BIKELANE_POCKETLANE_ADJACENT VARCHAR NULL
            ,BIKELANE_CONTRAFLOW VARCHAR NULL
            ,BIKELANE_CONVENTIONAL VARCHAR NULL
            ,BIKELANE_DUAL_PROTECTED VARCHAR NULL
            ,BIKELANE_DUAL_BUFFERED VARCHAR NULL
            ,BIKELANE_PROTECTED VARCHAR NULL
            ,BIKELANE_BUFFERED VARCHAR NULL
            ,DOUBLEYELLOW_LINE VARCHAR NULL
            ,SECTIONFLAGS VARCHAR NULL
            ,LOC_ERROR VARCHAR NULL
            ,MIDMEASURE NUMERIC NULL
            ,AADT NUMERIC NULL
            ,FHWAFUNCTIONALCLASS NUMERIC NULL
            ,HPMSID VARCHAR NULL
            ,HPMSSECTIONTYPE NUMERIC NULL
            ,ID VARCHAR NULL
            ,IRI NUMERIC NULL
            ,IRI_DATE TIMESTAMPTZ NULL
            ,NHSCODE NUMERIC NULL
            ,OWNERSHIP VARCHAR NULL
            ,PCI_CONDCATEGORY VARCHAR NULL
            ,PCI_LASTINSPECTED TIMESTAMPTZ NULL
            ,PCI_SCORE VARCHAR NULL
            ,SIDEWALK_IB_PAVTYPE VARCHAR NULL
            ,SIDEWALK_IB_WIDTH VARCHAR NULL
            ,SIDEWALK_OB_PAVTYPE VARCHAR NULL
            ,SIDEWALK_OB_WIDTH VARCHAR NULL
            ,SPEEDLIMITS_IB NUMERIC NULL
            ,SPEEDLIMITS_IB_ALT VARCHAR NULL
            ,SPEEDLIMITS_OB NUMERIC NULL
            ,SPEEDLIMITS_OB_ALT VARCHAR NULL
            ,SUBBLOCKID VARCHAR NULL
            ,BLOCKID VARCHAR NULL
            ,BLOCKKEY VARCHAR NULL
            ,DCFUNCTIONALCLASS NUMERIC NULL
            ,NHSTYPE VARCHAR NULL
            ,QUADRANT NUMERIC NULL
            ,STREETNAME VARCHAR NULL
            ,STREETTYPE VARCHAR NULL
            ,SNOWROUTE_DPW VARCHAR NULL
            ,SNOWZONE_DPW VARCHAR NULL
            ,SNOWROUTE VARCHAR NULL
            ,SNOWSECTION VARCHAR NULL
            ,SNOWZONE VARCHAR NULL
            ,LEFTTURN_CURBLANE_EXCL VARCHAR NULL
            ,LEFTTURN_CURBLANE_EXCL_LEN NUMERIC NULL
            ,RIGHTTURN_CURBLANE_EXCL VARCHAR NULL
            ,RIGHTTURN_CURBLANE_EXCL_LEN NUMERIC NULL
            ,TOTALBIKELANES NUMERIC NULL
            ,TOTALBIKELANEWIDTH NUMERIC NULL
            ,RPPDIRECTION VARCHAR NULL
            ,RPPSIDE VARCHAR NULL
            ,SLOWSTREETINFO VARCHAR NULL
            ,TOTALRAISEDBUFFERWIDTH VARCHAR NULL
            ,AADT_YEAR NUMERIC NULL
            ,SHAPELEN NUMERIC NULL
            ,SHAPE VARCHAR NULL
            ,geography geography null
    """
    ,'roadway_blockface':"""
            OBJECTID VARCHAR PRIMARY KEY
            ,ROUTEID VARCHAR NUll
            ,SIDE VARCHAR NUll
            ,MEAS_FROM NUMERIC NULL
            ,MEAS_TO NUMERIC NULL
            ,BLOCKFACEKEY VARCHAR NUll
            ,_OFFSET NUMERIC NULL
            ,SHAPELEN NUMERIC NULL
            ,SHAPE geometry
            ,geography geography null
        """
    ,'roadway_intersection_approach':"""
            OBJECTID VARCHAR PRIMARY KEY
            ,ROUTEID VARCHAR NUll
            ,FROMMEASURE NUMERIC NULL
            ,TOMEASURE NUMERIC NULL
            ,ROUTENAME VARCHAR NUll
            ,ROADTYPE VARCHAR NUll
            ,APPROACHID VARCHAR NUll
            ,TOTALTRAVELLANES NUMERIC NULL
            ,TOTALPARKINGLANES NUMERIC NULL
            ,TOTALRAISEDBUFFERS NUMERIC NULL
            ,TOTALTRAVELLANEWIDTH NUMERIC NULL
            ,TOTALCROSSSECTIONWIDTH NUMERIC NULL
            ,TOTALPARKINGLANEWIDTH NUMERIC NULL
            ,TOTALRAISEDBUFFERWIDTH NUMERIC NULL
            ,TOTALTRAVELLANESINBOUND NUMERIC NULL
            ,TOTALTRAVELLANESOUTBOUND NUMERIC NULL
            ,TOTALTRAVELLANESBIDIRECTIONAL NUMERIC NULL
            ,TOTALTRAVELLANESREVERSIBLE NUMERIC NULL
            ,SUMMARYDIRECTION VARCHAR NUll
            ,BIKELANE_PARKINGLANE_ADJACENT VARCHAR NUll
            ,BIKELANE_THROUGHLANE_ADJACENT VARCHAR NUll
            ,BIKELANE_POCKETLANE_ADJACENT VARCHAR NUll
            ,BIKELANE_CONTRAFLOW VARCHAR NUll
            ,BIKELANE_CONVENTIONAL VARCHAR NUll
            ,BIKELANE_DUAL_PROTECTED VARCHAR NUll
            ,BIKELANE_DUAL_BUFFERED VARCHAR NUll
            ,BIKELANE_PROTECTED VARCHAR NUll
            ,BIKELANE_BUFFERED VARCHAR NUll
            ,DOUBLEYELLOW_LINE VARCHAR NUll
            ,SECTIONFLAGS VARCHAR NUll
            ,LOC_ERROR VARCHAR NUll
            ,MIDMEASURE NUMERIC NULL
            ,INTERSECTIONID VARCHAR NUll
            ,AADT NUMERIC NULL
            ,AADT_YEAR NUMERIC NULL
            ,APPROACH_CARD_DIRECTION VARCHAR NUll
            ,APPROACH_INT_DIRECTION VARCHAR NUll
            ,APPROACH_LEG_ANGLE VARCHAR NUll
            ,FHWAFUNCTIONALCLASS VARCHAR NUll
            ,HMPSID VARCHAR NUll
            ,HPMSSECTIONTYPE VARCHAR NUll
            ,ID VARCHAR NUll
            ,IRI VARCHAR NUll
            ,IRI_DATE TIMESTAMPTZ NULL
            ,NHSCODE VARCHAR NUll
            ,OWNERSHIP VARCHAR NUll
            ,PCI_CONDCATEGORY VARCHAR NUll
            ,PCI_LASTINSPECTED TIMESTAMPTZ NULL
            ,PCI_SCORE NUMERIC NULL
            ,QUADRANT NUMERIC NULL
            ,SIDEWALK_IB_PAVTYPE VARCHAR NUll
            ,SIDEWALK_IB_WIDTH VARCHAR NULL
            ,SIDEWALK_OB_PAVTYPE VARCHAR NUll
            ,SIDEWALK_OB_WIDTH  VARCHAR NULL
            ,SPEEDLIMITS_IB NUMERIC NULL
            ,SPEEDLIMITS_IB_ALT VARCHAR NUll
            ,SPEEDLIMITS_OB NUMERIC NULL
            ,SPEEDLIMITS_OB_ALT VARCHAR NUll
            ,STREETNAME VARCHAR NUll
            ,STREETTYPE VARCHAR NUll
            ,SUBBLOCKID VARCHAR NUll
            ,BLOCKID VARCHAR NUll
            ,BLOCKKEY VARCHAR NUll
            ,DCFUNCTIONALCLASS VARCHAR NUll
            ,NHSTYPE VARCHAR NUll
            ,SUBBLOCKKEY VARCHAR NUll
            ,ANGLE VARCHAR NUll
            ,DIRECTIONALITY VARCHAR NUll
            ,INTERSECTIONDIRECTION VARCHAR NUll
            ,SNOWROUTE_DPW VARCHAR NUll
            ,SNOWZONE_DPW VARCHAR NUll
            ,SNOWROUTE VARCHAR NUll
            ,SNOWSECTION VARCHAR NUll
            ,SNOWZONE VARCHAR NUll
            ,LEFTTURN_CURBLANE_EXCL VARCHAR NUll
            ,LEFTTURN_CURBLANE_EXCL_LEN NUMERIC NULL
            ,RIGHTTURN_CURBLANE_EXCL VARCHAR NUll
            ,RIGHTTURN_CURBLANE_EXCL_LEN NUMERIC NULL
            ,TOTALBIKELANES NUMERIC NULL
            ,TOTALBIKELANEWIDTH NUMERIC NULL
            ,RPPDIRECTION VARCHAR NUll
            ,RPPSIDE VARCHAR NUll
            ,SLOWSTREETINFO VARCHAR NUll
            ,GLOBALID VARCHAR NUll
            ,SHAPELEN  NUMERIC NULL
            ,SHAPE geometry
            ,geography geography null
        """
    ,'smd_boundaries':"""
            OBJECTID VARCHAR NULL
            ,SMD_ID VARCHAR NULL
            ,ANC_ID VARCHAR NULL
            ,WEB_URL VARCHAR NULL
            ,NAME VARCHAR NULL
            ,CHAIR VARCHAR NULL
            ,REP_NAME VARCHAR NULL
            ,LAST_NAME VARCHAR NULL
            ,FIRST_NAME VARCHAR NULL
            ,ADDRESS VARCHAR NULL
            ,ZIP VARCHAR NULL
            ,EMAIL VARCHAR NULL
            ,Shape_Length NUMERIC NULL
            ,Shape_Area NUMERIC NULL
            ,MIDDLE_NAME VARCHAR NULL
            ,PHONE VARCHAR NULL
            ,geography geography null
        """
    ,'twitter':"""
            search_term VARCHAR NULL
            ,search_term_id VARCHAR NULL
            ,convo_group_id VARCHAR NULL
            ,tweet_id VARCHAR NULL
            ,tweet_text VARCHAR NULL
            ,reply_to_status VARCHAR NULL
            ,quoted_status VARCHAR NULL
            ,created_at TIMESTAMPTZ NULL
            ,user_id VARCHAR NULL
            ,user_screen_name VARCHAR NULL
            ,user_location VARCHAR NULL
            ,coordinates VARCHAR NULL
            ,user_place VARCHAR NULL
        """
    ,'vision_zero': """
            OBJECTID VARCHAR PRIMARY KEY
            ,GLOBALID   VARCHAR NULL 
            ,REQUESTID VARCHAR NULL
            ,REQUESTTYPE VARCHAR NULL
            ,REQUESTDATE TIMESTAMPTZ NULL
            ,STATUS VARCHAR NULL
            ,STREETSEGID VARCHAR NULL
            ,COMMENTS VARCHAR NULL
            ,USERTYPE VARCHAR NULL
            ,geography geography null
            """
    ,'ward_boundaries': """
            OBJECTID VARCHAR PRIMARY KEY
            ,WARD  VARCHAR NULL 
            ,NAME  VARCHAR NULL 
            ,REP_NAME  VARCHAR NULL 
            ,WEB_URL  VARCHAR NULL 
            ,REP_PHONE  VARCHAR NULL 
            ,REP_EMAIL  VARCHAR NULL 
            ,REP_OFFICE  VARCHAR NULL 
            ,WARD_ID  VARCHAR NULL 
            ,LABEL  VARCHAR NULL 
            ,AREASQMI  VARCHAR NULL 
            ,POP_2000  VARCHAR NULL 
            ,POP_2010  VARCHAR NULL 
            ,POP_2011_2015  VARCHAR NULL 
            ,POP_BLACK  VARCHAR NULL 
            ,POP_NATIVE_AMERICAN  VARCHAR NULL 
            ,POP_ASIAN  VARCHAR NULL 
            ,POP_HAWAIIAN  VARCHAR NULL 
            ,POP_OTHER_RACE  VARCHAR NULL 
            ,TWO_OR_MORE_RACES  VARCHAR NULL 
            ,NOT_HISPANIC_OR_LATINO  VARCHAR NULL 
            ,HISPANIC_OR_LATINO  VARCHAR NULL 
            ,POP_MALE  VARCHAR NULL 
            ,POP_FEMALE  VARCHAR NULL 
            ,AGE_0_5  VARCHAR NULL 
            ,AGE_5_9  VARCHAR NULL 
            ,AGE_10_14  VARCHAR NULL 
            ,AGE_15_17  VARCHAR NULL 
            ,AGE_18_19  VARCHAR NULL 
            ,AGE_20  VARCHAR NULL 
            ,AGE_21  VARCHAR NULL 
            ,AGE_22_24  VARCHAR NULL 
            ,AGE_25_29  VARCHAR NULL 
            ,AGE_30_34  VARCHAR NULL 
            ,AGE_35_39  VARCHAR NULL 
            ,AGE_40_44  VARCHAR NULL 
            ,AGE_45_49  VARCHAR NULL 
            ,AGE_50_54  VARCHAR NULL 
            ,AGE_55_59  VARCHAR NULL 
            ,AGE_60_61  VARCHAR NULL 
            ,AGE_65_66  VARCHAR NULL 
            ,AGE_67_69  VARCHAR NULL 
            ,AGE_70_74  VARCHAR NULL 
            ,AGE_75_79  VARCHAR NULL 
            ,AGE_80_84  VARCHAR NULL 
            ,AGE_85_PLUS  VARCHAR NULL 
            ,MEDIAN_AGE  VARCHAR NULL 
            ,UNEMPLOYMENT_RATE  VARCHAR NULL 
            ,TOTAL_HH  VARCHAR NULL 
            ,FAMILY_HH  VARCHAR NULL 
            ,PCT_FAMILY_HH  VARCHAR NULL 
            ,NONFAMILY_HH  VARCHAR NULL 
            ,PCT_NONFAMILY_HH  VARCHAR NULL 
            ,PCT_BELOW_POV  VARCHAR NULL 
            ,PCT_BELOW_POV_FAM  VARCHAR NULL 
            ,PCT_BELOW_POV_WHITE  VARCHAR NULL 
            ,PCT_BELOW_POV_BLACK  VARCHAR NULL 
            ,PCT_BELOW_POV_NAT_AMER  VARCHAR NULL 
            ,PCT_BELOW_POV_ASIAN  VARCHAR NULL 
            ,PCT_BELOW_POV_HAWAIIAN  VARCHAR NULL 
            ,PCT_BELOW_POV_OTHER  VARCHAR NULL 
            ,PCT_BELOW_POV_TWO_RACES  VARCHAR NULL 
            ,POP_25_PLUS  VARCHAR NULL 
            ,POP_25_PLUS_9TH_GRADE  VARCHAR NULL 
            ,POP_25_PLUS_GRADUATE  VARCHAR NULL 
            ,MARRIED_COUPLE_FAMILY  VARCHAR NULL 
            ,MALE_HH_NO_WIFE  VARCHAR NULL 
            ,FEMALE_HH_NO_HUSBAND  VARCHAR NULL 
            ,MEDIAN_HH_INCOME  VARCHAR NULL 
            ,PER_CAPITA_INCOME  VARCHAR NULL 
            ,PCT_BELOW_POV_HISP  VARCHAR NULL 
            ,PCT_BELOW_POV_WHTE_NOHISP  VARCHAR NULL 
            ,NO_DIPLOMA_25_PLUS  VARCHAR NULL 
            ,DIPLOMA_25_PLUS  VARCHAR NULL 
            ,NO_DEGREE_25_PLUS  VARCHAR NULL 
            ,ASSOC_DEGREE_25_PLUS  VARCHAR NULL 
            ,BACH_DEGREE_25_PLUS  VARCHAR NULL 
            ,MED_VAL_OOU  VARCHAR NULL 
            ,SHAPE_LENGTH0 NUMERIC NULL
            ,SHAPE_AREA0 NUMERIC NULL
            ,SHAPEAREA NUMERIC NULL
            ,SHAPELEN NUMERIC NULL
            ,geography geography null
            """

    }
    return data_model_dict[target_table]
