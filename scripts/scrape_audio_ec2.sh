#!/bin/bash
source my_venv/bin/activate
python3 transcribe_audio.py
python3 json_to_postgis.py folders source-data/scanner/transcribed/ --env prod --move_to_folder source-data/scanner/loaded-to-postgis/
python3 extract_scanner_audio_json.py --env prod   