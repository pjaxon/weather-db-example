## This script gets data from NOAA, stores it in file
## '/Users/chuckschultz/work/data/noaa_dump.json' and logs the transaction.
## Variables needed for api call:
##   dataset_id: (string) types of datasets GHCND is for Daily Summaries
##   data_types: (string) data labels (examples: TMIN, TMAX)
##   locations: (string) cities
##   stations: (string) weather station data
##   start_date: (string) query start date
##   end_date: (string) query end date
##   limit: (string) max number of entries in response to query

import requests
import datetime
import json
import time
noaa_token=''


# Set variables
header = {'token': noaa_token}
base_url = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
dataset_id = "?datasetid=GHCND"
data_types = ""
locations = ""
stations = ""
start_date = "&startdate=2020-01-01"
end_date = "&enddate=2020-01-03"
limit = "&limit=1000"

# Function to get number of entries
def get_count():
    try:
        limit = "&limit=1"
        url = base_url + dataset_id + data_types + locations + \
          stations + start_date + end_date + limit
        dump = requests.get(url, headers=header)
        json_data = json.loads(dump.content)
        return json_data["metadata"]["resultset"]["count"]
    except TypeError: # If there are no results
        print("Count:", None)
        with open('/Users/chuckschultz/work/data/noaa.log', 'a') as file: # log transaction
            file.write(str(datetime.datetime.now()) + "\nCount: None")
        return 0

# Get number of iterations for api calls based on count of entries
count = get_count()
if count == 0:
    iterations = 0
if count <= 1000:
    iterations = 1
elif count % 1000 == 0:
    iterations = count//1000
else:
    iterations = count//1000 + 1

# Function gets NOAA data, store in file '/Users/chuckschultz/work/data/noaa_dump.json'
# and log transaction in file '/Users/chuckschultz/work/data/noaa.log'
def get_noaa():
    off = 1
    try:
        for batch in range(iterations):
            time.sleep(10)
            offset = "&offset=" + str(off)
            url = base_url + dataset_id + data_types + locations + \
              stations + start_date + end_date + limit + offset
            dump = requests.get(url, headers=header)
      
            with open('/Users/chuckschultz/work/data/noaa_dump.json', 'wb') as file: # store data
                file.write(dump.content)
                print("Link:", url, "\nBatch:", str(batch + 1), " of ", \
                  str(iterations), "\tCount:", str(count))

                with open('/Users/chuckschultz/work/data/noaa.log', 'a') as file: # log transaction
                    file.write(str(datetime.datetime.now()) + "\nLink: " + url + "\nBatch: " + \
                      str(batch + 1) + " of " + str(iterations) + "\tCount: " + str(count) + "\n")
            off += 1000  
    except:
        with open('/Users/chuckschultz/work/data/noaa.log', 'a') as file: # log transaction
            file.write(str(datetime.datetime.now()) + "\nError")
        raise Exception("Error")

get_noaa()

