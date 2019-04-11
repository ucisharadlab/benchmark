import json
import time
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
    return int(time.mktime(timestamp.timetuple()))*1000

def loadJSON(filename):
    with open(filename, 'r') as dataFile:
        return json.load(dataFile)

def dumpJSON(filename, data):
    with open(filename, 'w') as writer:
        json.dump(data, writer, indent=4)

def toDatetime(datetimeStr):
    return datetime.datetime.strptime(datetimeStr, '%Y-%m-%d %H:%M:%S')

def toTime(timeStr):
    return datetime.datetime.strptime(timeStr, '%H:%M:%S')

def dumpTXT(filename, data):
    if 'G' in data:
        with open(filename+'_GroundTruth.txt', 'w') as writer:
            for i, g in enumerate(data['G']):
                writer.write(g)
                writer.write('\n')
                writer.write(str(data['G'][g]['wifiAP']))
                writer.write('\n')
                writer.write(str(data['G'][g]['location']))
                writer.write('\n')
                writer.write(str(data['G'][g]['time']))
                writer.write('\n')
                writer.write(str(data['G'][g]['user']))
                writer.write('\n')

    elif 'O' in data:
        with open(filename+'_Observations.txt', 'w') as writer:
            for i, o in enumerate(data['O']):
                writer.write(o)
                writer.write('\n')
                writer.write(str(data['O'][o]['wifiAP']))
                writer.write('\n')
                writer.write(str(data['O'][o]['time']))
                writer.write('\n')
                writer.write(str(data['O'][o]['user']))
                writer.write('\n')
