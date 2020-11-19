import pulse


WEBSITE_URL = "https://web.pulsepoint.org/DB/giba.php?agency_id=EMS1205"

def main():

    data = pulse.get_data()
    print(data)


if __name__ == "__main__":
    main()
