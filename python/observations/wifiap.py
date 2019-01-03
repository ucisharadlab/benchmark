import datetime
import random
import json
import uuid
import numpy as np
from utils import helper

from trajectory import TrajectoryScale


def createWiFiObservations(dt, end, step, dataDir):

    with open(dataDir + 'sensor.json') as data_file:
        data = json.load(data_file)

    sensors = []

    for sensor in data:
        if sensor['type_']['id'] == "WiFiAP":
            sensors.append(sensor)

    numSenors = len(sensors)

    with open(dataDir + 'platform.json') as data_file:
        platforms = json.load(data_file)

    clientIds = [platform['hashedMac'] for platform in platforms]
    numClients = len(clientIds)

    fpObj = open('data/wifiAPData.json', 'w')

    count = 0
    while dt < end:

        for i in np.random.choice(numSenors, numSenors/8, replace=False):
            for j in range(8):
                pickedSensor = helper.deleteSensorAttributes(sensors[i])

                id = str(uuid.uuid4())
                obs = {
                    "id": id,
                    "timeStamp": dt.strftime('%Y-%m-%d %H:%M:%S'),
                    "sensor": pickedSensor,
                    "payload": {
                        "clientId": clientIds[random.randint(0, numClients-1)]
                    }
                }
                fpObj.write(json.dumps(obs) + '\n')

                if count % 200000 == 0:
                    print ("{} Random WiFiAP Observations".format(count))
                count += 1

        dt += step

    fpObj.close()


def createIntelligentWiFiObs(dt, days, step, dataDir):
    trajectoryScale = TrajectoryScale(dataDir, "data/presenceData.json", "data/wifiAPData.json", "wifiMap.json",
                                      dt, days, step)
    trajectoryScale.generatePersonPaths()