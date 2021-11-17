#!/bin/bash
source my_venv/bin/activate
python3 get_historical_tweets_by_user.py --users rsprousenews alanhenney realtimenews10 
python3 json_to_postgis.py folders source-data/twitter/specific_users/ --env prod --move_to_folder source-data/twitter/loaded-to-postgis/
python3 extract_twitter_json.py --env prod
python3 generate_pulsepoint.py --env prod 
python3 classify_scanner_audio.py --env prod 
# python3 generate_pulsepoint_analysis.py --env prod
# python3 generate_crashes_all_sources.py --env prod
python3 csv_to_postgis.py folders source-data/dc-fss --env prod --clean_columns yes
python3 generate_dc_fss.py --env prod 