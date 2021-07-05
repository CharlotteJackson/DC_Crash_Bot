#!/bin/bash
source my_venv/bin/activate
python3 scrape_pulsepoint.py
python3 parse_pulsepoint.py --items_to_parse source-data/pulsepoint/unparsed/ --move_after_parsing yes --move_to_folder source-data/pulsepoint/unparsed/converted/ --parsed_destination source-data/pulsepoint/files_for_loading/
python3 s3_to_postgis.py folders source-data/pulsepoint/files_for_loading/  --mode append --header true --move_after_loading yes --move_to_folder source-data/pulsepoint/loaded_to_postgis/
# python3 generate_pulsepoint.py --env prod
# python3 generate_pulsepoint_analysis.py