import datetime
import random
import json
import uuid
import numpy as np


def createPresence(dt, end, step, dataDir):

    with open(dataDir + 'virtualSensor.json') as data_file:
        vs = json.load(data_file)
    for v in vs:
        if v['type_']['id'] == "WiFiToPresence":
            pickedSensor = v
            break

    with open(dataDir + 'sensor.json') as data_file:
        data = json.load(data_file)
    sensors = []
    for sensor in data:
        if sensor['type_']['id'] == "WiFiAP":
            sensors.append(sensor)
    numSenors = len(sensors)

    with open(dataDir + 'infrastructure.json') as data_file:
        rooms = json.load(data_file)
    with open(dataDir + 'user.json') as data_file:
        users = json.load(data_file)

    numRooms = len(rooms)
    numUsers = len(users)

    fpObj = open('data/presenceData.json', 'w')

    print ("Creating Random Presence Data")

    count = 0
    while dt < end:
        for j in np.random.choice(numUsers, numUsers/2, replace=False):
            id = str(uuid.uuid4())
            sobs = {
                "id": id,
                "timeStamp": dt.strftime('%Y-%m-%d %H:%M:%S'),
                "virtualSensor": pickedSensor,
                "type_": pickedSensor["type_"]["semanticObservationType"],
                "semanticEntity": users[j],
                "payload": {
                    "location": rooms[random.randint(0, numRooms-1)]['id']
                }
            }
            fpObj.write(json.dumps(sobs) + '\n')
            
            if count % 200000 == 0:
                print ("{} Random Presence Observations".format(count))
            count += 1

        dt += step

    fpObj.close()
