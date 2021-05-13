# Usage
# python transcribe_audio.py

# Python imports
import json
import logging
import requests
from pydub import AudioSegment
import time
from typing import Type, Union, Dict, Any, List

import speech_recognition as sr

# postgress imports
# import psycopg2
# import pandas.io.sql as psql

# aws imports
import boto3
from botocore.exceptions import ClientError

# AWS resoucres
s3 = boto3.client("s3")
transcribe = boto3.client("transcribe")

# TODO change this to your bucket
S3_AUDIO_BUCKET = "banjo-private-bucket"


def upload_file_to_s3(file_name: str, bucket: str, object_name: str = None) -> bool:
    """
    Purpose:
        Uploads a file to s3
    Args/Requests:
         file_name: name of file
         bucket: s3 bucket name
         object_name: s3 name of object
    Return:
        Status: True if uploaded, False if failure
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    try:
        response = s3.upload_file(file_name, bucket, object_name)
        logging.info(response)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def save_json(json_path: str, json_data: Any) -> None:
    """
    Purpose:
        Save json files
    Args:
        path_to_json (String): Path to  json file
        json_data: Data to save
    Returns:
        N/A
    """
    # save sut config
    try:
        with open(json_path, "w") as outfile:
            json.dump(json_data, outfile)
    except Exception as error:
        raise OSError(error)


def get_audio_files(timestamp):
    """
    Purpose:
        Gets list of audio files
    Args:
        N/A
    Returns:
        audio_calls - List of audio call objects
    """

    # api_url = "https://api.openmhz.com/dcfd/calls/older?time=EPOCH_TIME_MIL"
    # need to pad three zeros to timestamp
    api_url = f"https://api.openmhz.com/dcfd/calls/older?time={timestamp}000"

    logging.info(f"Calling this url {api_url}")

    calls = requests.get(api_url).json()
    audio_recods = []
    for call in list(calls["calls"]):

        talk_group = int(call["talkgroupNum"])

        # We're interested in talk group 101 (dispatch) and 728/729 (EMS 5 and 6)
        valid_talk_groups = [101, 728, 729]

        if talk_group not in valid_talk_groups:
            continue

        # TODO will need to pass on calls we already have ids on

        call_id = call["_id"]
        audio_url = call["url"]
        timestamp = call["time"]
        call_length = call["len"]

        audio_data = {}
        audio_data["id"] = call_id
        audio_data["source"] = talk_group
        audio_data["audio_url"] = audio_url
        audio_data["timestamp"] = timestamp
        audio_data["call_length"] = call_length

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


def local_transcibe_auido_file(audio_file):
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


def test_transcibe_local():
    """
    Purpose:
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
    text = local_transcibe_auido_file(wav_file)
    print(text)


def test_transcibe_aws():
    """
    Purpose:
        Test transibing a file with aws
    Args:
        N/A
    Returns:
        N/A
    """

    audio_file = download_audio_file(
        "https://s3.us-east-2.wasabisys.com/openmhz/media/dcfd-729-1619900237.m4a"
    )

    print(f"Downloaded {audio_file}")

    upload_file_to_s3(audio_file, S3_AUDIO_BUCKET, f"raw_audio/{audio_file}")
    s3_url = f"s3://{S3_AUDIO_BUCKET}/raw_audio/{audio_file}"
    transcribed_audio = aws_transcribe_job(audio_file, s3_url)
    print(transcribed_audio)


def aws_transcribe_job(file_name, s3_url):
    """
    Purpose:
        transcibes a file using aws transcribe
    Args:
        N/A
    Returns:
        N/A
    """
    transcribe.start_transcription_job(
        TranscriptionJobName=file_name,
        Media={"MediaFileUri": s3_url},
        MediaFormat="mp4",
        LanguageCode="en-US",
    )

    # TODO possibile infinite loop condtion?
    while True:
        status = transcribe.get_transcription_job(TranscriptionJobName=file_name)
        if status["TranscriptionJob"]["TranscriptionJobStatus"] in [
            "COMPLETED",
            "FAILED",
        ]:
            break
        time.sleep(10)
        logging.info(status)

    if status["TranscriptionJob"]["TranscriptionJobStatus"] == "COMPLETED":
        data = requests.get(
            status["TranscriptionJob"]["Transcript"]["TranscriptFileUri"]
        ).json()
        text = data["results"]["transcripts"][0]["transcript"]
        logging.info(text)
        return text

    return ""


def transcribe_pipeline():
    """
    Purpose:
        Gets all the files to be recorded
    Args:
        N/A
    Returns:
        N/A
    """

    # Get calls in the past hour, that we care about
    epoch_time = int(time.time())
    audio_data = get_audio_files(epoch_time)
    logging.info(audio_data)

    # for all files transicbe the audio
    for call in audio_data:

        # down the file
        file_name = download_audio_file(call["audio_url"])

        # upload the file to s3
        upload_file_to_s3(file_name, S3_AUDIO_BUCKET, f"raw_audio/{file_name}")

        s3_url = f"s3://{S3_AUDIO_BUCKET}/raw_audio/{file_name}"

        # start transcription job
        transcribed_audio = aws_transcribe_job(file_name, s3_url)
        call["transcribed_audio"] = transcribed_audio

    # TODO you can save this to database directly
    save_json(f"transcribed_audio_{epoch_time}.json", audio_data)

    # TODO cleanup function
    # want to remove downloaded m4a files, and the file in s3 bucket


def main():
    """
    Purpose:
        Transcribes audio files
    Args:
        N/A
    Returns:
        N/A
    """
    logging.info("Getting audio files")
    # audio_records = get_audio_files()
    # print(audio_records)

    # test_transcibe()
    # test_transcibe_aws()

    transcribe_pipeline()


if __name__ == "__main__":
    log_level = logging.INFO
    logging.basicConfig(
        format="%(asctime)s | %(levelname)s: %(message)s", level=log_level
    )
    main()
