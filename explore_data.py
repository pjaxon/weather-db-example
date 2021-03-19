## This script queries the db and does exploratory data analysis
import os
import psycopg2
from psycopg2.extras import DictCursor
import requests
from datetime import datetime
import json
import pandas as pd

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

# query = 'SELECT COUNT(*) FROM weather.noaa_raw'
# cur.execute(query)
# results = cur.fetchall()
# print(results)

query = 'SELECT * FROM weather.stations_raw LIMIT 10'
cur.execute(query)
results = cur.fetchall()
for result in results:
    print(result)