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
limit = "&limit=10"

# Function gets NOAA station data (118,487 entries - 25 at a time),
# store in file '/Users/chuckschultz/work/data/station_dump.json' and
# log transaction in file '/Users/chuckschultz/work/data/noaa_stations.log'
def get_noaa_stations():
    off = 1
    try:
        for batch in range(2): # range(119)
            time.sleep(1)
            offset = "&offset=" + str(off)
            url = base_url + dataset_id + limit + offset
            dump = requests.get(url, headers=header)
            j = dump.json()
            # print (dump.json()) # j = r.json()
            for result in j['data']:
                try:
                    # insert_sql = "INSERT INTO pipedrive.deals_api (deal_id, deal_json) VALUES (%s, (regexp_replace(%s::text, '\\u0000', '', 'g'))::jsonb) ON CONFLICT (deal_id) DO UPDATE SET deal_json = (regexp_replace(%s::text, '\\u0000', '', 'g'))::jsonb "
                    print (result)
                        try:
                            #cur.execute(insert_sql,  ( deal['id'],  json.dumps(deal, indent=4, sort_keys=True),  json.dumps(deal, indent=4, sort_keys=True) ) )
                            print ('it worked')
                        except:
                            print ( "Could not insert deal json")
                            traceback.print_exc()
                except:
                    print ('could not iterate through results')

    except TypeError: # If there are no results
        print("Count:", None)
        with open('/home/theraceblogger/temp_data/noaa_stations.log', 'a') as file: # log transaction
            file.write(str(datetime.datetime.now()) + "\nCount: None")

get_noaa_stations()
