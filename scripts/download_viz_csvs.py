import sqlalchemy 
import pandas as pd 
import os 
from connect_to_rds import get_connection_strings, create_postgres_engine

# Add the data folder to path library, to store downloads 
cur_path = os.path.dirname(__file__)
# cur_path = os.getcwd()
print(cur_path)
path_parent = os.path.dirname(cur_path)
print(path_parent)
data_folder = os.path.join(path_parent,'data')
print(data_folder)


# create db connection
dbname='postgres'
env="DEV"
engine = create_postgres_engine(destination="AWS_PostGIS", target_db=dbname, env=env)
db_credentials = get_connection_strings("AWS_PostGIS")

# grab names of all tables in Viz schema, store in a list
get_tables_query = """
SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'viz'
"""
# put column names of source table in list
tables_to_download = [r for (r,) in engine.execute(get_tables_query).fetchall()]

for table in tables_to_download:
    df = pd.read_sql('select * from viz.{}'.format(table), engine, coerce_float=False)
    download_path = os.path.join(data_folder, table+'.csv')
    df.to_csv(download_path, index=False)
    print('downloaded table ', table,' with ',len(df),' records')