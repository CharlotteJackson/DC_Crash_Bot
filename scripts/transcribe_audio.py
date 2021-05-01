# Usage
# python transcribe_audio.py

# Python imports
import json
import os
import logging
import requests
from pydub import AudioSegment


import speech_recognition as sr

# postgress imports
# import psycopg2
# import pandas.io.sql as psql

# # gdrive imports
# import gspread
# from oauth2client.service_account import ServiceAccountCredentials

# # aws imports
# import boto3
# from botocore.exceptions import ClientError
# from requests import api


def get_audio_files():
    """
    Purpose:
        Gets list of audio files
    Args:
        N/A
    Returns:
        audio_calls - List of audio call objects
    """

    api_url = "https://api.openmhz.com/dcfd/calls/"

    # TODO figure out cadence to run this?
    # when we call we store the timestamp (epoch time in miliseconds)
    # we want to get all new calls since the last time we ran
    # api_url = "https://api.openmhz.com/dcfd/calls/newer?time=EPOCH_TIME_MIL"

    calls = requests.get(api_url).json()
    audio_recods = []
    for call in list(calls["calls"]):

        talk_group = call["talkgroupNum"]  # TODO where are these numbers mapped?
        audio_url = call["url"]
        timestamp = call["time"]
        call_length = call["len"]

        # TODO we would transcribe this audio with a service
        transcibed_audio = "This text has been transcribed"

        audio_data = {}
        audio_data["source"] = talk_group
        audio_data["audio_url"] = audio_url
        audio_data["timestamp"] = timestamp
        audio_data["call_length"] = call_length
        audio_data["transcribed_audio"] = transcibed_audio

        audio_recods.append(audio_data)

    return audio_recods


def download_audio_file(audio_file):
    """
    Purpose:
        Downloads an audio file
    Args:
        audio_file - location of audio file
    Returns:
        file_name - name of saved file
    """

    # https://s3.us-east-2.wasabisys.com/openmhz/media/dcfd-729-1619900237.m4a

    file_name = audio_file.replace(
        "https://s3.us-east-2.wasabisys.com/openmhz/media/", ""
    )

    r = requests.get(audio_file)
    with open(file_name, "wb") as f:
        f.write(r.content)

    return file_name


def convert_m4a_to_wav(audio_file):
    """
    Purpose:
        Converts an .m4a file to .wav
    Args:
        audio_file - location of audio file
    Returns:
        file_name - name of saved file
    """

    audio_dst = audio_file.replace(".m4a", ".wav")
    sound = AudioSegment.from_file(audio_file, format="m4a")
    sound.export(audio_dst, format="wav")
    return audio_dst


def transicbe_auido_file(audio_file):
    """
    Purpose:
        Converts .wav file to text
    Args:
        audio_file - location of audio file
    Returns:
        text - text of audio
    """
    r = sr.Recognizer()

    audio_data = sr.AudioFile(audio_file)
    with audio_data as source:
        audio = r.record(source)

    text = r.recognize_google(audio)

    return text


def test_transcibe():
    """
    Test transibing a file
    Args:
        N/A
    Returns:
        N/A
    """

    audio_file = download_audio_file(
        "https://s3.us-east-2.wasabisys.com/openmhz/media/dcfd-729-1619900237.m4a"
    )

    print(f"Downloaded {audio_file}")
    wav_file = convert_m4a_to_wav(audio_file)
    print(wav_file)
    text = transicbe_auido_file(wav_file)
    print(text)


def main():
    """
    Transcribes audio files
    Args:
        N/A
    Returns:
        N/A
    """
    logging.info("Getting audio files")
    # audio_records = get_audio_files()
    # print(audio_records)

    test_transcibe()


if __name__ == "__main__":
    log_level = logging.INFO
    logging.basicConfig(
        format="%(asctime)s | %(levelname)s: %(message)s", level=log_level
    )
    main()
