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
    # Convert all NaNs
    df["scanner_audio_missing"] = df["scanner_audio_missing"].fillna(0).astype(int)
    df["comp_plan_area"] = df["comp_plan_area"].fillna("N/A")
    df["nbh_cluster_names"] = df["nbh_cluster_names"].fillna("N/A")
    df["ward_name"] = df["ward_name"].fillna("N/A")
    df["address"] = df["address"].fillna("N/A")

    cols = df.columns

    json_obj = []

    df.apply(lambda row: turn_row_to_json_obj(row, json_obj, cols), axis=1)

    try:
        final_obj = {}
        final_obj["crashes"] = json_obj

        with open("data/february_2021_crashes.json", "w") as outfile:
            json.dump(final_obj, outfile)
    except Exception as error:
        raise OSError(error)


if __name__ == "__main__":
    main()