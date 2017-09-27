import requests
import json
import datetime as dt
from datetime import datetime
import mysql.connector as mconn


SOURCE_SERVER = "http://sensoria.ics.uci.edu:8001"
DEST_SERVER = "http://localhost:8001"
SOURCE_DATABSE = "tippersweb.ics.uci.edu"


def getObsData():
    start_time = datetime(2017, 9, 7, 14, 45, 0)
    try:
        while True:
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json"
            }
            path = "/observation/get"
            args = "?start_timestamp=" + start_time.strftime("%Y-%m-%d %H:%M:%S")
            url = SOURCE_SERVER + path + args

            print(url)
            start_time = datetime.now()

            response = requests.get(SOURCE_SERVER + path + args, headers=headers)
            observations = json.loads(response.text)

            for obs in observations:
                print (obs)

    except Exception as e:
        print (e)


def getObsDataFromMySQL():
    conn = mconn.connect(host=SOURCE_DATABSE, port=3306, database='tippersdb', user='test', passwd='test')
    cur = conn.cursor()

    query = "SELECT * From OBSERVATION WHERE timeStamp>%s AND timeStamp<%s"

    start_time = datetime(2017, 9, 7, 14, 45, 0)
    while start_time < datetime(2017, 9, 7, 15, 5, 0):

        cur.execute(query, (start_time, start_time + dt.timedelta(minutes = 5)))
        for row in cur.fetchall():
            print(row)
        start_time += dt.timedelta(minutes=5)

#getObsData()
getObsDataFromMySQL()