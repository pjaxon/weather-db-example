## This script gets data from NOAA, and stores it in weather.raw
import os
import psycopg2
from psycopg2.extras import DictCursor
import requests
from datetime import datetime
import json
import time

# Set variables
noaa_token = os.environ['noaa_token']
header = {'token': noaa_token}
base_url = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
dataset_id = "?datasetid=GHCND"
station_id = "&stationid="
station = "GHCND:AEM00041217"
start_date = "&startdate="
end_date = "&enddate="
limit = "&limit=1000"
offset = "&offset="

# Function that returns mindate and maxdate for a given station
def get_station_params(station):
    query = f"SELECT sr.station_jsonb ->> 'mindate', sr.station_jsonb ->> 'maxdate' FROM weather.stations_raw sr WHERE sr.station_id = '{station}'"
    cur.execute(query)
    result = cur.fetchall()
    return result[0][0], result[0][1]

# Function that iterates through a year and loads data
def load_data(url, off_set=1):
    try:
        time.sleep(.5)
        r = requests.get(url, headers=header)
        j = r.json()
        for result in j['results']:
            try:
                print(result)
            except:
                print ('could not iterate through results')
        off_set += 1000
        if (off_set <= j['metadata']['resultset']['count']):
            url = base_url + dataset_id + station_id + station + start_date + start + end_date + end + limit + offset + off_set
            load_data(url, off_set)
    except:
        print('Function failed')

# Function gets NOAA data and loads into database
def get_noaa(station):
    start, end = get_station_params(station)
    start_dt = datetime.strptime(start, '%Y-%m-%d')
    end_dt = datetime.strptime(end, '%Y-%m-%d')
    num_years = end_dt.year - start_dt.year + 1

    for year in range(num_years):
        if num_years == 1:
            url = base_url + dataset_id + station_id + station + start_date + start + end_date + end + limit + offset + off_set
            load_data(url)

        elif year == 0:
            url = base_url + dataset_id + station_id + station + start_date + start + end_date + start_dt.year + "-12-31" + limit + offset
            load_data(url)

        elif year == num_years - 1:
            url = base_url + dataset_id + station_id + station + start_date + end_dt.year + "-01-01" + end_date + end + limit + offset
            load_data(url)

        else:
            url = base_url + dataset_id + station_id + station + start_date + str(start_dt.year+year) + "-01-01" + end_date + str(start_dt.year+year) + "-12-31" + limit + offset
            load_data(url)

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

get_noaa(station)