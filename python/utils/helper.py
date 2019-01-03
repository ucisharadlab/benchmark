import time
import json
import csv
import datetime

def deleteSensorAttributes(sensor):
    try:
        del sensor['infrastructure']
        del sensor['owner']
        del sensor['coverage']
        del sensor['sensorConfig']
        del sensor['type_']['description']
        del sensor['type_']['mobility']
        del sensor['type_']['captureFunctionality']
        del sensor['type_']['payloadSchema']

    except:
        pass
    return sensor


def deleteSOTypeAttributes(type_):
    try:
        del type_['description']
        del type_['payloadSchema']
    except:
        pass
    return type_

def deleteVirtualSensorAttributes(sensor):
    try:
        del sensor['description']
        del sensor['language']
        del sensor['type_']
        del sensor['projectName']
    except:
        pass
    return sensor

def deleteUserAttributes(user):
    try:
        del user['groups']
        del user['googleAuthToken']
    except:
        pass
    return user


def deleteInfraAttributes(infra):
    try:
        del infra['geometry']
        del infra['type_']['description']
    except:
        pass
    return infra


def toUTC(timestamp):
    return int(time.mktime(timestamp.timetuple()))


def jsonToCsv(src, dest):
    json_data = json.load(open(src, "r"))
    # open a file for writing
    csv_data = open(dest, 'w')
    # create the csv writer object
    csvwriter = csv.writer(csv_data)
    count = 0

    for row in json_data:

        if count == 0:
            header = row.keys()
            csvwriter.writerow(header)
            count += 1
        csvwriter.writerow(row.values())

    csv_data.close()

def jsonToCsvLine(src, dest):
    json_data = open(src, "r")
    # open a file for writing
    csv_data = open(dest, 'w')
    # create the csv writer object
    csvwriter = csv.writer(csv_data, quotechar="\"")
    count = 0

    for row in json_data:
        if not row.startswith("{"): continue
        row = json.loads(row)
        if count == 0:
            header = row.keys()
            csvwriter.writerow(header)
            count += 1
        csvwriter.writerow(row.values())

    csv_data.close()


def jsonToCsvPresenceLine(src, dest):
    json_data = open(src, "r")
    # open a file for writing
    csv_data = open(dest, 'w')
    # create the csv writer object
    csvwriter = csv.writer(csv_data, quotechar="\"")
    count = 0

    for row in json_data:
        if not row.startswith("{"): continue
        row = json.loads(row)
        if count == 0:
            header = row.keys()
            csvwriter.writerow(["id", "start_timestamp", "end_timestamp", "user", "location"])
            count += 1
        csvwriter.writerow([row["id"], row["start_timestamp"], row["end_timestamp"], row["user"], row["location"]])

    csv_data.close()


def jsonToCsvOccupancy(src, dest):
    json_data = open(src, "r")
    # open a file for writing
    csv_data = open(dest, 'w')
    # create the csv writer object
    csvwriter = csv.writer(csv_data, quotechar="\"")
    count = 0

    for row in json_data:
        if not row.startswith("{"): continue
        row = row.strip().strip(",")
        row = json.loads(row)
        del row['occupancy']
        #row['timeStamp'] = toUTC(row['timeStamp'])
        if count == 0:
            header = row.keys()
            csvwriter.writerow(header)
            count += 1
        csvwriter.writerow(row.values())

    csv_data.close()


def jsonToCsvOccupancyFromDict(json_data, dest):
    csv_data = open(dest, 'w')
    csvwriter = csv.writer(csv_data, quotechar="\"")
    count = 0

    for row in json_data:
        del row['occupancy']
        #row['timeStamp'] = toUTC(row['timeStamp'])
        if count == 0:
            csvwriter.writerow(["id", "timeStamp", "location", "green_badges", "yellow_badges", "red_badges"])
            count += 1
        csvwriter.writerow([row["id"], row["timeStamp"], row["location"], row["green_badges"], row["yellow_badges"], row["red_badges"]])
    csv_data.close()