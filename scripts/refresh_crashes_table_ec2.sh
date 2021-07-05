#!/bin/bash
source my_venv/bin/activate
python3 get_all_dc_open_data.py --dataset crashes_raw --urls all --formats csv
python3 get_all_dc_open_data.py --dataset crash_details --urls all --formats csv
python3 csv_to_postgis.py folders source-data/dc-open-data/crashes_raw --env prod  --clean_columns yes
python3 csv_to_postgis.py folders source-data/dc-open-data/crash_details --env prod
python3 stg_to_source_data.py --env prod "crashes_raw_complete.csv" crashes_raw truncate 
python3 stg_to_source_data.py --env prod "crash_details_complete.csv" crash_details truncate   
python3 generate_dc_crashes_with_details.py --env prod