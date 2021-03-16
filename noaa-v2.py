## This script gets data from NOAA, and stores it in weather.raw
import os
import psycopg2
from psycopg2.extras import DictCursor
import requests
import datetime
import json
import time

# Set variables
noaa_token = os.environ['noaa_token']
header = {'token': noaa_token}
base_url = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
dataset_id = "?datasetid=GHCND"
data_types = ""
locations = ""
station = "GHCND:AEM00041217"
start_date = "&startdate=1983-01-02"
end_date = "&enddate=2021-03-10"
limit = "&limit=1000"

def get_station_params(station):
    min_query = f"SELECT sr.station_jsonb ->> 'mindate' FROM weather.stations_raw sr WHERE sr.station_id = '{station}'"
    #max_query = f"SELECT sr.station_jsonb ->> 'maxdate' FROM weather.stations_raw sr WHERE sr.station_id = '{station}'"
    cur.execute(min_query)
    start = cur.fetchall()
    return start[0][0]


# Function gets NOAA data and loads into database
def get_noaa(entry_number = 1):
    try:
        time.sleep(.5)
        offset = "&offset=" + str(entry_number)
        url = base_url + dataset_id + "&stationid=" + station + start_date + end_date + limit + offset
        print('Starting entry num: ' + str(entry_number) + ', ' + url)
        r = requests.get(url, headers=header)
        j = r.json()

        for result in j['results']:
            try:
                print(result)
                # insert_sql = "INSERT INTO weather.stations_raw (station_id, station_jsonb) VALUES (%s,%s) ON CONFLICT (station_id) DO UPDATE SET station_jsonb = %s"
                # cur.execute(insert_sql, (result['id'], json.dumps(result, indent=4, sort_keys=True), json.dumps(result, indent=4, sort_keys=True))) 
            except:
                print ('could not iterate through results')
        
        entry_number += 1000       
        if (entry_number < j['metadata']['resultset']['count']): 
            #print('get_noaa_stations looping')
            get_noaa(entry_number)
                
    except:
        print('Function failed')


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

minimum = get_station_params(station)
print(minimum)

#get_noaa()