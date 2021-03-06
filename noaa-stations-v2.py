## This script gets 118,487 stations data from NOAA, and stores it in weather.stations_raw
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
base_url = "https://www.ncdc.noaa.gov/cdo-web/api/v2/stations"
dataset_id = "?datasetid=GHCND"
limit = "&limit=1000"

# Function gets NOAA station data (118,487 entries - 1000 at a time) and loads into database
def get_noaa_stations(entry_number = 1):
    try:
        time.sleep(.5)
        offset = "&offset=" + str(entry_number)
        url = base_url + dataset_id + limit + offset
        print('Starting entry num: ' + str(entry_number) + ', ' + url)
        r = requests.get(url, headers=header)
        j = r.json()

        for result in j['results']:
            try:
                insert_sql = "INSERT INTO weather.stations_raw (station_id, station_jsonb) VALUES (%s,%s) ON CONFLICT (station_id) DO UPDATE SET station_jsonb = %s"
                cur.execute(insert_sql, (result['id'], json.dumps(result, indent=4, sort_keys=True), json.dumps(result, indent=4, sort_keys=True))) 
            except:
                print ('could not iterate through results')
        
        entry_number += 1000       
        if (entry_number <= j['metadata']['resultset']['count']): 
            #print('get_noaa_stations looping')
            get_noaa_stations(entry_number)
                
    except:
        print('Function failed')

# Function that connects to database
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
