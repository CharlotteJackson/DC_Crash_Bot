#!/bin/bash
source my_venv/bin/activate
python3 scrape_citizen.py
python3 json_to_postgis.py folders source-data/citizen/unparsed/ --env prod --move_to_folder source-data/citizen/loaded-to-postgis/
python3 extract_citizen_json.py --env prod 
python3 generate_citizen.py --env prod