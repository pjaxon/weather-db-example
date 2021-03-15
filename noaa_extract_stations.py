## This script gets 118,487 stations data from NOAA, stores it in file
## '/Users/chuckschultz/work/data/station_dump.json' and logs the transaction
import os
import requests
import datetime
import json
import time
noaa_token = os.environ['noaa_token']


# Set variables
header = {'token': noaa_token}
base_url = "https://www.ncdc.noaa.gov/cdo-web/api/v2/stations"
dataset_id = "?datasetid=GHCND"
limit = "&limit=1000"

# Function gets NOAA station data (118,487 entries - 25 at a time),
# store in file '/Users/chuckschultz/work/data/station_dump.json' and
# log transaction in file '/Users/chuckschultz/work/data/noaa_stations.log'
def get_noaa_stations():
    off = 1
    try:
        for batch in range(2): # range(119)
            time.sleep(10)
            offset = "&offset=" + str(off)
            url = base_url + dataset_id + limit + offset
            dump = requests.get(url, headers=header)

            with open(f'/home/theraceblogger/temp_data/station_dump{batch + 1}.json', 'wb') as file: # store data
                file.write(dump.content)
            json_data = json.loads(dump.content)
            count = json_data["metadata"]["resultset"]["count"]
            print("Link:", url, "\nBatch:", str(batch + 1), " of 119 \tCount:", str(count))

            with open('/home/theraceblogger/temp_data/noaa_stations.log', 'a') as file: # log transaction
                file.write(str(datetime.datetime.now()) + "\nLink: " + url + "\nBatch: " + \
                    str(batch + 1) + " of 119 \tCount: " + str(count) + "\n")
            off += 1000

    except TypeError: # If there are no results
        print("Count:", None)
        with open('/home/theraceblogger/temp_data/noaa_stations.log', 'a') as file: # log transaction
            file.write(str(datetime.datetime.now()) + "\nCount: None")

get_noaa_stations()

