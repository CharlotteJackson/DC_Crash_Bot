#!/bin/bash
source my_venv/bin/activate
python3 scrape_waze.py
python3 json_to_postgis.py folders source-data/waze/unparsed/ --env prod --move_to_folder source-data/waze/loaded-to-postgis/
python3 extract_waze_json.py --env prod