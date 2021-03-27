import pandas as pd
import json


def turn_row_to_json_obj(row, json_obj, cols):

    temp_obj = {}

    for col in cols:
        temp_obj[col] = row[col]

    json_obj.append(temp_obj)


def main():
    print("Converting csv to json")

    df = pd.read_csv("data/february_2021_crashes.csv")

    cols = df.columns

    json_obj = []

    df.apply(lambda row: turn_row_to_json_obj(row, json_obj, cols), axis=1)

    try:
        with open("data/february_2021_crashes.json", "w") as outfile:
            json.dump(json_obj, outfile)
    except Exception as error:
        raise OSError(error)


if __name__ == "__main__":
    main()