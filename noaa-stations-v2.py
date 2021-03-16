## This script gets 118,487 stations data from NOAA, stores it in file
## '/Users/chuckschultz/work/data/station_dump.json' and logs the transaction
import os
import psycopg2
from psycopg2.extras import DictCursor
import requests
import datetime
import json
import time
noaa_token = os.environ['noaa_token']

max_page_number = 119

# Set variables
header = {'token': noaa_token}
base_url = "https://www.ncdc.noaa.gov/cdo-web/api/v2/stations"
dataset_id = "?datasetid=GHCND"

limit = "&limit=1000"

# Function gets NOAA station data (118,487 entries - 25 at a time),
# store in file '/Users/chuckschultz/work/data/station_dump.json' and
# log transaction in file '/Users/chuckschultz/work/data/noaa_stations.log'
def get_noaa_stations(page_number = 0):
    # batch_total = 0
    global max_page_number
    
    try:
            time.sleep(.5)
            offset = "&offset=" + str(page_number)
            url = base_url + dataset_id + limit + offset
            dump = requests.get(url, headers=header)
            j = dump.json()
            # print (dump.json()) # j = r.json()
            for result in j['results']:
                try:
                    # print (result)
                    insert_sql = "INSERT INTO weather.stations_raw (station_id, station_jsonb) VALUES (%s,%s) ON CONFLICT (station_id) DO UPDATE SET station_jsonb = %s"
                    cur.execute(insert_sql,  ( result['id'],  json.dumps(result, indent=4, sort_keys=True),  json.dumps(result, indent=4, sort_keys=True) ) )
                    # records_processed += 1
                    
                except:
                    print ('could not iterate through results')

                    
            if (page_number < max_page_number): 
                page_number += 1
                get_noaa_stations(page_number)
                
    except TypeError: # If there are no results
        print("Count:", None)
        with open('/home/theraceblogger/temp_data/noaa_stations.log', 'a') as file: # log transaction
            file.write(str(datetime.datetime.now()) + "\nCount: None")

def db_connect():
  db_name = os.environ['db_name']
  db_user = os.environ['db_user']
  db_host = os.environ['db_host']
  db_credentials = os.environ['db_creds']
 
  conn_string = "dbname='" + str(db_name) + "' user='" + str(db_user) + "' host='" + str(db_host) + "' password='" + str(db_credentials) + "'"

  try:
    conn = psycopg2.connect(str(conn_string))
    conn.autocommit = True
  except:
    print("Unable to connect to the database")

  cur = conn.cursor(cursor_factory=DictCursor)
  return cur

cur = db_connect()

get_noaa_stations()
