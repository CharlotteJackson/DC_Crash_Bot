from connect_to_rds import get_connection_strings, create_postgres_engine
import subprocess
import sys
import argparse
import os


def create_new_db(target_env:str,target_db_name:str, copy_from_env:str):
    # connect to the db that will be copied from
    engine = create_postgres_engine(destination="AWS_PostGIS", env=copy_from_env.upper())
    db_credentials = get_connection_strings("AWS_PostGIS")
    # get prod master credentials
    prod_db_host =db_credentials[copy_from_env.upper()]['HOST']
    prod_db_port = db_credentials[copy_from_env.upper()]['PORT']
    prod_db_name = db_credentials[copy_from_env.upper()]['DB']
    prod_db_uid =db_credentials[copy_from_env.upper()]['UID']
    prod_db_pwd = db_credentials[copy_from_env.upper()]['PWD']
    # get testdb credentials
    test_db_host =db_credentials[target_env.upper()]['HOST']
    test_db_port = db_credentials[target_env.upper()]['PORT']
    test_db_name = db_credentials[target_env.upper()]['DB']
    test_db_uid =db_credentials[target_env.upper()]['UID']
    test_db_pwd = db_credentials[target_env.upper()]['PWD']
    test_db_users = db_credentials[target_env.upper()]['USERS']

    kill_db_query="""
    SELECT	pg_terminate_backend (pid)
    FROM	pg_stat_activity
    WHERE	pg_stat_activity.datname = '{0}';
    """.format(target_db_name)

    # kill
    # engine.execute(kill_db_query)

    # drop
    command = 'DROP DATABASE IF EXISTS {}'.format(target_db_name)
    os_system_arg='PGPASSWORD=\'{}\' psql --host={} --port={} --username={} --dbname={} --no-password --command=\"{}\"'.format(prod_db_pwd,prod_db_host, prod_db_port, prod_db_uid, prod_db_name, command)
    os.system(os_system_arg)     

    # create
    command = 'CREATE DATABASE {}'.format(target_db_name)
    os_system_arg='PGPASSWORD=\'{}\' psql --host={} --port={} --username={} --dbname={} --no-password --command=\"{}\"'.format(test_db_pwd,test_db_host, test_db_port, test_db_uid, 'postgres',command)
    os.system(os_system_arg)   

    # create users on new db
    for user_pwd in test_db_users:
        user=list(user_pwd.keys())[0]
        pwd=user_pwd[user]
        command = 'CREATE ROLE {} WITH LOGIN ENCRYPTED PASSWORD \'{}\';'.format(user,pwd)
        os_system_arg='PGPASSWORD=\'{}\' psql -h {} -p {} -U {} --dbname={} --no-password --command=\"{}\"'.format(test_db_pwd,test_db_host, test_db_port, test_db_uid, test_db_name,command)
        print(os_system_arg)
        os.system(os_system_arg)

    # install PostGIS extensions
    command = """
    CREATE EXTENSION aws_s3 CASCADE;
    CREATE EXTENSION postgis;
    CREATE EXTENSION fuzzystrmatch;
    CREATE EXTENSION postgis_tiger_geocoder;
    CREATE EXTENSION postgis_topology;
    """
    os_system_arg='PGPASSWORD=\'{}\' psql -h {} -p {} -U {} --dbname={} --no-password --command=\"{}\"'.format(test_db_pwd,test_db_host, test_db_port, test_db_uid, test_db_name,command)
    os.system(os_system_arg)
    # alter schemas 
    command="""
    ALTER SCHEMA tiger OWNER TO rds_superuser;
    ALTER SCHEMA tiger_data OWNER TO rds_superuser;
    ALTER SCHEMA topology OWNER TO rds_superuser;
    """
    os_system_arg='PGPASSWORD=\'{}\' psql -h {} -p {} -U {} --dbname={} --no-password --command=\"{}\"'.format(test_db_pwd,test_db_host, test_db_port, test_db_uid, test_db_name,command)
    os.system(os_system_arg)
    # create function
    command = 'CREATE FUNCTION exec(text) returns text language plpgsql volatile AS \$f\$ BEGIN EXECUTE \$1; RETURN \$1; END; \$f\$;'
    os_system_arg='PGPASSWORD=\'{}\' psql -h {} -p {} -U {} --dbname={} --no-password --command=\"{}\"'.format(test_db_pwd,test_db_host, test_db_port, test_db_uid, test_db_name,command)
    os.system(os_system_arg)
    # execute function
    command="""
    SELECT exec('ALTER TABLE ' || quote_ident(s.nspname) || '.' || quote_ident(s.relname) || ' OWNER TO rds_superuser;')
        FROM (
            SELECT nspname, relname
            FROM pg_class c JOIN pg_namespace n ON (c.relnamespace = n.oid) 
            WHERE nspname in ('tiger','topology') AND
            relkind IN ('r','S','v') ORDER BY relkind = 'S')
        s;
    """
    os_system_arg='PGPASSWORD=\'{}\' psql -h {} -p {} -U {} --dbname={} --no-password --command=\"{}\"'.format(test_db_pwd,test_db_host, test_db_port, test_db_uid, test_db_name,command)
    os.system(os_system_arg)

    # get all schemas on prod db
    get_schemas_query = """
    SELECT DISTINCT table_schema
    FROM information_schema.tables 
    WHERE is_insertable_into = 'YES' 
    AND table_schema not like 'pg_%%' 
    AND table_schema not in ('tiger', 'tiger_data', 'topology', 'aws_commons', 'aws_s3','information_schema','my_new_topo','public')
    """
    schemas = [r for (r,) in engine.execute(get_schemas_query).fetchall()]
    print(schemas)

    # create engine on test db
    test_engine = create_postgres_engine(destination="AWS_PostGIS", env=target_env.upper())

    # create schemas
    for schema in schemas:
        create_schema_query = """
        CREATE SCHEMA IF NOT EXISTS {0};
        GRANT ALL PRIVILEGES ON SCHEMA {0} TO PUBLIC;
        """.format(schema)
        print(create_schema_query)
        test_engine.execute(create_schema_query)


def refresh_db(env:str, from_env:str):
    engine = create_postgres_engine(destination="AWS_PostGIS", env=env.upper())
    db_credentials = get_connection_strings("AWS_PostGIS")
    # db_users = db_credentials[env.upper()]['USERS']
    prod_db_host =db_credentials[from_env.upper()]['HOST']
    prod_db_uid =db_credentials[from_env.upper()]['UID']
    prod_db_pwd =db_credentials[from_env.upper()]['PWD']
    prod_db_port = db_credentials[from_env.upper()]['PORT']
    prod_db_name = db_credentials[from_env.upper()]['DB']
    prod_engine = create_postgres_engine(destination="AWS_PostGIS", env=from_env.upper())

    create_fdw_query = """
        BEGIN;
        CREATE EXTENSION IF NOT EXISTS postgres_fdw;
        DROP SERVER IF EXISTS prod CASCADE;
        CREATE SERVER prod FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host '{prod_db_host}', dbname '{prod_db_name}');
        COMMIT;
    """.format(prod_db_name=prod_db_name, prod_db_host=prod_db_host)

    engine.execute(create_fdw_query)

    #  create user mappings
    map_user_query = """
    CREATE USER MAPPING FOR PUBLIC
        SERVER prod
        OPTIONS (user '{user}', password '{pwd}');
    """.format(user=prod_db_uid, pwd=prod_db_pwd)

    engine.execute(map_user_query)

    # pull the schemas off the viz copy of the prod database
    get_schemas_query = """
    SELECT DISTINCT table_schema
    FROM information_schema.tables 
    WHERE is_insertable_into = 'YES' 
    AND table_schema not like 'pg_%%' 
    AND table_schema not in ('tiger', 'tiger_data', 'topology', 'aws_commons', 'aws_s3','information_schema','my_new_topo','public')
    """
    schemas = [(r, 'prod_'+r) for (r,) in prod_engine.execute(get_schemas_query).fetchall()]
    print(schemas)

    # map schemas 
    for source_schema, destination_schema in schemas:
        create_schema_query = """
        CREATE SCHEMA IF NOT EXISTS {destination_schema};
        GRANT ALL PRIVILEGES ON SCHEMA {destination_schema} TO PUBLIC;
        IMPORT FOREIGN SCHEMA {source_schema}
            FROM SERVER prod
            INTO {destination_schema};
        """.format(source_schema=source_schema, destination_schema=destination_schema)

        engine.execute(create_schema_query)

    # pull all the tables from prod db
    get_schemas_tables_query = """
    SELECT DISTINCT table_schema,table_name 
    FROM information_schema.tables 
    WHERE is_insertable_into = 'YES' 
    AND table_schema not like 'pg_%%' 
    AND table_schema not in ('tiger', 'tiger_data', 'topology', 'aws_commons', 'aws_s3','information_schema','my_new_topo','public')
    """
    schemas_tables = [(schema,table) for (schema,table) in prod_engine.execute(get_schemas_tables_query).fetchall()]

    #  create and populate tables
    for schema, table in schemas_tables:
        create_populate_tables_query="""
        CREATE TABLE IF NOT EXISTS {schema}."{table}" (LIKE prod_{schema}."{table}");

        DELETE FROM {schema}."{table}";

        INSERT INTO {schema}."{table}"
            SELECT * FROM prod_{schema}."{table}";

        GRANT ALL PRIVILEGES ON {schema}."{table}" TO PUBLIC;
        """.format(schema=schema, table=table)

        print(create_populate_tables_query)
        engine.execute(create_populate_tables_query)

    # create all the indexes
    get_indexes_tables_query = """
    SELECT DISTINCT indexdef
    FROM pg_indexes
    WHERE schemaname not like 'pg_%%' 
    AND schemaname not in ('tiger', 'tiger_data', 'topology', 'aws_commons', 'aws_s3','information_schema','my_new_topo','public')
    """
    indexes = [indexdef for (indexdef,) in prod_engine.execute(get_indexes_tables_query).fetchall()]

    #  create and populate tables
    for indexdef in indexes:
        print(indexdef)
        try:
            engine.execute(indexdef)
        except:
            continue

if __name__ == "__main__":
    # first create new prod db
    # create_new_db(target_db_name='postgres', target_env='PROD', copy_from_env='PROD_OLD')
    # refresh_db(env='PROD', from_env='PROD_OLD')
    # print("new DB copied from old DB")
    # then create new test db
    # create_new_db(target_db_name='postgres_dev', target_env='DEV', copy_from_env='DEV_OLD')
    refresh_db(env='DEV', from_env='DEV_OLD')
    print("new test DB created")

