## This script connects to our database and gets data from NOAA
import os
import psycopg2
from psycopg2.extras import DictCursor
import requests
import json
import traceback


# Connect to database
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

dwh_cur = db_connect()


# Pull data from database

def select_data():
	test_sql = "select * from weather.emdat_america_disasters LIMIT 5"

	dwh_cur.execute(test_sql)
	rows = dwh_cur.fetchall()
	for row in rows:
	   print(row['Dis No'])


# Push data to database

#def insert_data():
#	test_sql = "INSERT INTO weather.noaa_stations_raw VALUES ('/home/theraceblogger/temp_data/station_dump1.json')"
#	dwh_cur.execute(test_sql)

def insert_data():
        test_sql = "CREATE TEMP TABLE weather_stations(info json);"
        dwh_cur.execute(test_sql)
        test_sql = "UPDATE weather_stations SET content 'cat /home/theraceblogger/temp_data/station_dump1.json';"
        dwh_cur.execute(test_sql)
        test_sql = "INSERT INTO weather.noaa_stations_raw VALUES (:'content');"
        dwh_cur.execute(test_sql)
insert_data()


# Extract NOAA data

def get_noaa_data():
  try:
    noaa_token = os.environ['noaa_token']
    header = {'token': noaa_token}
    url = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND&locationid=ZIP:80455&startdate=2020-01-01&enddate=2020-01-31&limit=10"
    r = requests.get(url, headers=header)
    print(r.content)
  except:
    print('You fucked something up!')
    traceback.print_exc()

#get_noaa_data()


# Extract WHO data

def get_who_data():
  try:
    url = "https://ghoapi.azureedge.net/api/Dimension/"
    r = requests.get(url)
    print(r.content)
  except:
    print('You fucked something up!')
    traceback.print_exc()

#get_who_data()
