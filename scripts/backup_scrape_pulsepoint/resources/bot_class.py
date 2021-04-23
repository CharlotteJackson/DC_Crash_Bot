# -*- coding: utf-8 -*-
"""
Created on April 19 2021

@edited by: Theo G
"""

##############################################################################
# imports
##############################################################################
from time import time, sleep
import pandas as pd
import re
import random
from os import chdir, getcwd
import pickle
import pandas as pd
wd = getcwd()  # lets you navigate using chdir within jupyter/spyder
chdir(wd)
import warnings
import os.path
from os import path
warnings.simplefilter(action='ignore', category=FutureWarning)


from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

from selenium.webdriver.chrome.options import Options

chrome_options = Options()
chrome_options.add_argument('--disable-background-timer-throttling')
chrome_options.add_argument('--disable-backgrounding-occluded-windows')
chrome_options.add_argument('--disable-background-timer-throttling')
chrome_options.add_argument('--disable-renderer-backgrounding')
chrome_options.add_argument('detach:True')
# chrome_options.add_argument('--headless')
# chrome_options.add_argument('--no-sandbox')
# chrome_options.add_argument('--disable-gpu')


##############################################################################
# functions
##############################################################################


def hasXpath(bot, xpath):
    try:
        bot.find_element_by_xpath(xpath)
        return True
    except:
        try:
            bot.find_element_by_css_selector(xpath)
            return True
        except:
            return False


def sleep_for(opt1, opt2):
    time_for = random.uniform(opt1, opt2)
    time_for_int = int(round(time_for))
    sleep(abs(time_for_int - time_for))
    for i in range(time_for_int, 0, -1):
        sleep(1)


##############################################################################
# bot
##############################################################################


class PulsePointBot:

    def __init__(self):
        self.bot = webdriver.Chrome(
            ChromeDriverManager().install(), chrome_options=chrome_options)

    def bot_run(self):
        bot = self.bot
        # get site
        bot.get('https://web.pulsepoint.org/')
        # wait for the settings button to appear
        wait_settings = WebDriverWait(bot, 10).until(
            lambda d: d.find_element_by_id("pp_wa_navbar_search_button"))
        print('settings button found')
        sleep_for(4, 6)

        # which agency
        select_agency = bot.find_element_by_xpath(
            '//input[@placeholder="Search agencies to view"]')
        sleep_for(2, 3)

        # type in wash dc
        select_agency.send_keys(
            'Washington DC - Office of Unified Communications (DC)')
        sleep(1)
        # press enter
        select_agency.send_keys(Keys.ENTER)
        sleep(2)

        # display incidents button
        incidents_button = bot.find_element_by_xpath(
            '//button[@class="pp_wa_large_button"]')
        incidents_button.click()
        sleep_for(4, 6)
        
        # click recent incidents tab
        incidents_tab = bot.find_element_by_xpath(
            '//li[@id="recent_incidents_tab"]')
        incidents_tab.click()
        sleep_for(2, 4)

        boxes_of_incidents = bot.find_elements_by_xpath(
            '//dd[@class="pp_incident_item_dd"]')

        # df to append to
        df = pd.DataFrame()

        for box in boxes_of_incidents:
            title = box.find_element_by_xpath(
                './/h2[@class="pp_incident_item_description_title"]').text
            location = box.find_element_by_xpath(
                './/h3[@class="pp_incident_item_description_location"]').text
            units = box.find_element_by_xpath(
                './/h6[@class="pp_incident_item_description_units"]').text
            time = box.find_element_by_xpath(
                './/h5[@class="pp_incident_item_timestamp_time"]').text
            day = box.find_element_by_xpath(
                './/h5[@class="pp_incident_item_timestamp_day"]').text
            duration = box.find_element_by_xpath(
                './/h6[@class="pp_incident_item_call_duration"]').text

            temp_df = pd.DataFrame([{'title': title, 'location': location,
                                     'units': units,
                                     'time': time,
                                     'day': day,
                                     'duration': duration}])
            # print(temp_df)
            df = df.append(temp_df)

        # clean up empty rows
        df = df[pd.notnull(df['title'])]

        bot.quit()
        return df


