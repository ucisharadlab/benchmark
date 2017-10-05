import datetime
import random
import json
import uuid

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

    fpObj = open('data/wifiAPdata.json', 'w')

    while dt < end:

        for i in range(numSenors/8):
            for j in range(numClients/8):
                pickedSensor = sensors[random.randint(0, numSenors - 1)]
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
        dt += step

    fpObj.close()