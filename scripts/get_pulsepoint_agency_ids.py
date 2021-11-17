import requests
from tqdm import tqdm
from time import sleep
import pandas as pd
import pulse
import string


output_path = 'existing_agency_ids_new.csv'
tableau_boundaries_output_path = 'agency_ids_boundaries_new.csv'

# create list
num_list = []

# add agency IDs that start with EMS
# for i in range(999, 1500):
#     i = str(i).zfill(4)
#     num_list.append('EMS'+str(i))

# add agency IDs that don't start with EMS
# for i in range(1, 99999):
#     i = str(i).zfill(5)
#     num_list.append(i)

# agency IDs that are two numbers with a letter in the middle (17M15 - Seattle)
for front_num in range(11,100):
    for letter in list(string.ascii_uppercase):
        for back_num in range(0,100):
            front_num=str(front_num).zfill(2)
            back_num=str(back_num).zfill(2)
            full_id = front_num+letter+back_num
            num_list.append(full_id)

# agency IDs that are two letters followed by 3 numbers (GB803 - El Paso)
for front_letter in list(string.ascii_uppercase):
    for back_letter in list(string.ascii_uppercase):
        for number in range(0,1000):
            number=str(number).zfill(3)
            full_id = front_letter+back_letter+number
            num_list.append(full_id)

payload={}
headers = {}

# dict of lists for agencies that exist
exists_list = {
            'Agency_ID':[]
            ,'Max_Lat':[]
            ,'Min_Lat':[]
            ,'Max_Long':[]
            ,'Min_Long':[]
    }

# a differently formatted dict of lists for making boundaries in tableau
tableau_boundaries = {
            'Agency_ID':[]
            ,'Point_Order':[]
            ,'Latitude':[]
            ,'Longitude':[]
    }

for i in tqdm(num_list):
    # refresh lists of incident lat/longs
    lats=[]
    longs=[]
    # ping the URL
    url = "https://web.pulsepoint.org/DB/giba.php?agency_id={}".format(str(i))
    response = requests.request("GET", url, headers=headers, data=payload)
    # print(i)
    # print(str(len(response.text)))

    # check if it returns real stuff
    if len(response.text) > 200:
        # append the agency ID to the list of existing agency IDs
        exists_list['Agency_ID'].append(i)
        # then pull the data back to parse it for lat/longs
        data = pulse.get_data(url)
        # active and recent incidents are in separate dictionaries
        record_status_types = [i for i in data['incidents'].keys() if i != 'alerts']
        for status in record_status_types:
            if data['incidents'][status] is not None:
                records_to_parse = [i for i in data['incidents'][status]]
                if records_to_parse is not None and len(records_to_parse)>0:
                    # loop through all incidents with the given status and put their lat/longs in a list
                    for record in records_to_parse:
                        lats.append(float(record['Latitude']))
                        longs.append(float(record['Longitude']))
        # remove incorrect values (latitudes <= 0, longitudes >=0)
        print(i)
        print(len(lats), " records in lats pre-filter")
        print(len(longs), " records in longs pre-filter")
        lats = [i for i in lats if i > 0]
        longs = [i for i in longs if i < 0]
        print(len(lats), " records in lats post-filter")
        print(len(longs), " records in longs post-filter")
        if len(lats)>0 and len(longs)>0:
            # then append max/min of lat/long to the output list
            exists_list['Max_Lat'].append(max(lats))
            exists_list['Min_Lat'].append(min(lats))
            exists_list['Max_Long'].append(max(longs))
            exists_list['Min_Long'].append(min(longs))
            # then append the same info in a different format to the tableau boundaries file
            # max lat/max long
            tableau_boundaries['Agency_ID'].append(i)
            tableau_boundaries['Point_Order'].append(1)
            tableau_boundaries['Latitude'].append(max(lats))
            tableau_boundaries['Longitude'].append(max(longs))
            # min lat/max long
            tableau_boundaries['Agency_ID'].append(i)
            tableau_boundaries['Point_Order'].append(2)
            tableau_boundaries['Latitude'].append(min(lats))
            tableau_boundaries['Longitude'].append(max(longs))
            # min lat/min long
            tableau_boundaries['Agency_ID'].append(i)
            tableau_boundaries['Point_Order'].append(3)
            tableau_boundaries['Latitude'].append(min(lats))
            tableau_boundaries['Longitude'].append(min(longs))
            # max lat/min long
            tableau_boundaries['Agency_ID'].append(i)
            tableau_boundaries['Point_Order'].append(4)
            tableau_boundaries['Latitude'].append(max(lats))
            tableau_boundaries['Longitude'].append(min(longs))
            # max lat/max long (again)
            tableau_boundaries['Agency_ID'].append(i)
            tableau_boundaries['Point_Order'].append(5)
            tableau_boundaries['Latitude'].append(max(lats))
            tableau_boundaries['Longitude'].append(max(longs))
        else:
            exists_list['Max_Lat'].append('NA')
            exists_list['Min_Lat'].append('NA')
            exists_list['Max_Long'].append('NA')
            exists_list['Min_Long'].append('NA')


    # sleep 3 seconds before repeating
    # sleep(3)
        
# make df from dict of lists
df = pd.DataFrame.from_dict(exists_list)    
tableau_df = pd.DataFrame.from_dict(tableau_boundaries)    
    
# export
df.to_csv(output_path, index=False)
tableau_df.to_csv(tableau_boundaries_output_path, index=False)