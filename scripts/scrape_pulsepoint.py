import json
import pulse


# DC pulsepoint website
WEBSITE_URL = "https://web.pulsepoint.org/DB/giba.php?agency_id=EMS1205"


def main():

    data = pulse.get_data()
    print(data)

    # # save data to example file
    # with open("../data/pulsepoint.json", "w") as outfile:
    #     json.dump(data, outfile)

if __name__ == "__main__":
    main()
