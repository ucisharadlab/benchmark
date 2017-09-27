import requests
import time
import json
from datetime import datetime
import re
import uuid

SERVER = "http://sensoria.ics.uci.edu:8001/api/query/select"
DATADIR = "data/TQL/"

COLLECTIONS = {
    "Location", "Region", "InfrastructureType", "Infrastructure", "User", "Group",
    "ObservationType", "SensorType", "Sensor", "PlatformType", "Platform"
}


def getData(collectionName):
    data = {
        "query": "SELECT ALL FROM {};".format(collectionName),
        "type": "TQL",
    }
    print (data)
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    response = requests.post(SERVER, json.dumps(data), headers=headers)

    if response.status_code == 200:
        print("{} Get Success {}".format(collectionName, time.clock()))
        with open(DATADIR + collectionName + ".json", "w") as w:
            w.write(str(json.loads(response.text)[:-1]).replace("type", "type_"))
    else:
        print("{} Get Failed {}".format(collectionName, response.status_code))


def getObservationData():
    collectionName = "Observation"
    data = {
        "query": "SELECT ALL FROM {};".format(collectionName),
        "type": "TQL",
    }
    print (data)
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    response = requests.post(SERVER, json.dumps(data), headers=headers)

    if response.status_code == 200:
        print("{} Get Success {}".format(collectionName, time.clock()))
        with open(DATADIR + collectionName + ".json", "w") as w:
            observations = json.loads(response.text)[:-1]
            strings = []
            for observation in observations:
                observation["timeStamp"] = \
                    "datetime('"+datetime.strptime(observation["timeStamp"], "%a %b %d %H:%M:%S %Z %Y")\
                        .strftime("%Y-%d-%mT%H:%M:%SZ") + "')"
                strings.append(str(observation).replace("type", "type_")
                               .replace('"'+observation["timeStamp"]+'"', observation["timeStamp"])
                               .replace('"', '\\"')
                               .replace("'", '"'))

            w.write("{}".format('\n'.join(strings)))

    else:
        print("{} Get Failed {}".format(collectionName, response.status_code))


def getSensorMap():
    data = {
        "query": "SELECT ALL FROM {};".format("Sensor"),
        "type": "TQL",
    }
    print (data)
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    response = requests.post(SERVER, json.dumps(data), headers=headers)
    print(response.text)
    sensors = json.loads(response.text)[:-1]
    sensorMap = {}
    for sensor in sensors:
        sensorMap[sensor['id']] = sensor

    return sensorMap


def getTypeMap():
    data = {
        "query": "SELECT ALL FROM {};".format("ObservationType"),
        "type": "TQL",
    }
    print (data)
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    response = requests.post(SERVER, json.dumps(data), headers=headers)

    types = json.loads(response.text)[:-1]
    typeMap = {}
    for type in types:
        typeMap[type['id']] = type

    return typeMap


def getObservationDataFromFile():

    sensors = getSensorMap()
    types = getTypeMap()

    with open("IoT-Database-Benchmark/ScaleData/SimulatedData/" + "temperaturesObsSim.json", "r") as r:
        observations = json.loads(r.read())
        strings = []
        for observation in observations:
            observation["timeStamp"] = \
                "datetime('"+datetime.strptime(observation["timestamp"], "%Y-%m-%d %H:%M:%S")\
                        .strftime("%Y-%m-%dT%H:%M:%SZ") + "')"
            observation['type'] = types[observation['typeId']]
            observation['sensor'] = sensors[observation['sensorId']]
            del observation['timestamp']
            del observation['typeId']
            del observation['sensorId']
            observation['id'] = str(uuid.uuid4())

            strings.append(str(observation).replace("type", "type_")
                           .replace('"'+observation["timeStamp"]+'"', observation["timeStamp"])
                           .replace('"', '\\"')
                           .replace("'", '"'))
    collectionName = "Observation"
    with open(DATADIR + collectionName + ".json", "w") as w:
        w.write("{}".format('\n'.join(strings)))





def getCompleteData():
    for collection in COLLECTIONS:
        getData(collection)


#getCompleteData()
#getObservationData()
getObservationDataFromFile()