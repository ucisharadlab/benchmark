import datetime
import random
import json
import uuid
import numpy as np

from utils import helper
from extrapolate import Scale, SemanticScale

MAX_OCCUPANCY = 100


def createOccupancy(dt, end, step, dataDir):

    with open(dataDir + 'virtualSensor.json') as data_file:
        vs = json.load(data_file)
    for v in vs:
        if v['type_']['id'] == "WiFiToOccupancy":
            pickedSensor = v

    with open(dataDir + 'sensor.json') as data_file:
        data = json.load(data_file)

    sensors = []

    for sensor in data:
        if sensor['type_']['id'] == "WiFiAP":
            sensors.append(sensor)

    numSenors = len(sensors)

    with open(dataDir + 'infrastructure.json') as data_file:
        rooms = json.load(data_file)

    numRooms = len(rooms)

    fpObj = open('data/occupancyData.json', 'w')

    print ("Creating Random Occupancy Data" + str(numRooms))
    type_ = pickedSensor["type_"]["semanticObservationType"]
    type_ = helper.deleteSOTypeAttributes(type_)
    pickedSensor = helper.deleteVirtualSensorAttributes(pickedSensor)
    count = 0
    while dt < end:

        for j in np.random.choice(numRooms, numRooms/2, replace=False):
            id = str(uuid.uuid4())
            sobs = {
                "id": id,
                "timeStamp": dt.strftime('%Y-%m-%d %H:%M:%S'),
                "virtualSensor": pickedSensor,
                "type_": type_,
                "semanticEntity": helper.deleteInfraAttributes(rooms[j]),
                "payload": {
                    "occupancy": random.randint(0, MAX_OCCUPANCY)
                }
            }
            fpObj.write(json.dumps(sobs) + '\n')

            if count % 200000 == 0:
                print ("{} Random Occupancy Observations".format(count))
            count += 1

        dt += step

    fpObj.close()


def createIntelligentOccupancy(origDays, extendDays, origSpeed, extendSpeed, speedScaleNoise,
                               timeScaleNoise, dataDir):

    with open(dataDir + 'semanticObservation.json') as data_file:
        observations = json.load(data_file)

    seedFile = open("data/seedPresence.json", "w")
    for observation in observations:
        if observation['type_']['id'] == "occupancy":
            seedFile.write(json.dumps(observation) + '\n')
    seedFile.close()

    seedFile = "data/seedPresence.json"
    outputFile = "data/occupancyData.json"
    scale = SemanticScale(dataDir, seedFile, outputFile, origDays, extendDays, origSpeed, extendSpeed,
                          "occupancy", speedScaleNoise, timeScaleNoise, int)

    scale.speedScale()
    scale.timeScale()
