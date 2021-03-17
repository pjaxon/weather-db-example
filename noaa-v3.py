## This script gets data from NOAA, and stores it in weather.raw
import os
import psycopg2
from psycopg2.extras import DictCursor
import requests
from datetime import datetime
import json
import time


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
off_set = 1

# Function that returns mindate and maxdate for a given station
def get_station_params(station):
    query = f"SELECT sr.station_jsonb ->> 'mindate', sr.station_jsonb ->> 'maxdate' FROM weather.stations_raw sr WHERE sr.station_id = '{station}'"
    cur.execute(query)
    result = cur.fetchall()
    return result[0][0], result[0][1]

start, end = get_station_params(station)
print(start, end)
print(station)

url = base_url + dataset_id + station_id + station + start_date + start + end_date + "1983-12-31" + limit + offset + str(off_set)
print(url)
# Function that iterates through a year and loads data
def load_data(url, off_set=1):
    try:
        time.sleep(1)
        r = requests.get(url, headers=header)
        j = r.json()
        print(j['metadata']['resultset']['count'])
        for result in j['results']:
            try:
                print(result)
                #print(url)
                # insert_sql = "INSERT INTO weather.noaa_raw (station_id, date, data_type, noaa_jsonb) VALUES (%s,%s,%s,%s) ON CONFLICT (station_id, date, data_type) DO UPDATE SET noaa_jsonb = %s"
                # cur.execute(insert_sql, (result['station'], result['date'], result['datatype'], json.dumps(result, indent=4, sort_keys=True), json.dumps(result, indent=4, sort_keys=True)))
            except:
                print ('could not iterate through results')
        off_set += 1000
        if (off_set <= j['metadata']['resultset']['count']):
            url = base_url + dataset_id + station_id + station + start_date + start + end_date + "1983-12-31" + limit + offset + str(off_set)
            load_data(url, off_set)
    except:
        print('Function failed\n', url)


load_data(url)


# Function gets NOAA data and loads into database
def get_noaa(station, off_set=1):
    start, end = get_station_params(station)
    start_dt = datetime.strptime(start, '%Y-%m-%d')
    end_dt = datetime.strptime(end, '%Y-%m-%d')
    num_years = end_dt.year - start_dt.year + 1

    for year in range(num_years):
        if num_years == 1:
            url = base_url + dataset_id + station_id + station + start_date + start + end_date + end + limit + offset + str(off_set)
            load_data(url)

        elif year == 0:
            url = base_url + dataset_id + station_id + station + start_date + start + end_date + str(start_dt.year) + "-12-31" + limit + offset + str(off_set)
            load_data(url)

        elif year == num_years - 1:
            url = base_url + dataset_id + station_id + station + start_date + str(end_dt.year) + "-01-01" + end_date + end + limit + offset + str(off_set)
            load_data(url)

        else:
            url = base_url + dataset_id + station_id + station + start_date + str(start_dt.year+year) + "-01-01" + end_date + str(start_dt.year+year) + "-12-31" + limit + offset + str(off_set)
            load_data(url)



# Function that gets each station id and loads data for each station
def load_db():
    query = "SELECT sr.station_id FROM weather.stations_raw sr"
    cur.execute(query)
    result = cur.fetchall()
    for station in result:
        get_noaa(station[0])

#load_db()