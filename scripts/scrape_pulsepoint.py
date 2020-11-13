from bs4 import BeautifulSoup
import requests
import json
import uuid
import datetime
import boto3



WEBSITE_URL = "https://web.pulsepoint.org/?agency_id=EMS1205"

def main():
    print("Scrape now")

    cookies = {"agencies":"EMS1205"}
    page = requests.get(WEBSITE_URL,cookies=cookies)
    #need to set cookies
    
    html = page.text
    print(html)
    soup = BeautifulSoup(html, "html.parser")


if __name__ == "__main__":
    main()
