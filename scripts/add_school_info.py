import sqlalchemy
from connect_to_rds import get_connection_strings, create_postgres_engine

dbname='postgres'
env="DEV"
engine = create_postgres_engine(destination="AWS_PostGIS", target_db=dbname, env=env)
db_credentials = get_connection_strings("AWS_PostGIS")

def add_school_info(engine, from_schema:str, from_table:str, target_schema:str, target_table:str):

    # empty variable to store list of table columns
    columns_string =''

    # get column names of source table
    get_columns_query = """
    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{0}' AND TABLE_NAME = '{1}'
    """.format(from_schema, from_table)

    # put column names of source table in list
    columns = [r for (r,) in engine.execute(get_columns_query).fetchall()]
    columns_string+='a.'+columns[0]
    for column in columns[1:]:
        columns_string+=' ,a.'+column

    add_school_query="""
        DROP TABLE IF EXISTS {0}.{1};
        CREATE TABLE {0}.{1}
        AS (

        SELECT 
            ARRAY_AGG(distinct b.school_name) as near_schools
        , MAX(b.ES) as ES
        , MAX(b.MS) as MS
        , MAX(b.HS) as HS
        , MAX(b.public_school) as public_school
        , MAX(b.charter_school) as charter_school
        , a.* 
        FROM {2}.{3} a
        LEFT JOIN analysis_data.all_schools b on ST_DWithin(b.geography,a.geography,200)
        GROUP BY {5}
        ) ;

        CREATE INDEX {4} ON {0}.{1} USING GIST (geography);
    """.format(target_schema, target_table, from_schema, from_table, target_schema+'_'+target_table+'_index', columns_string)
    
    engine.execute(add_school_query)