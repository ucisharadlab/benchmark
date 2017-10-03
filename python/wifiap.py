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

    num = len(sensors)

    clientIds = ["client_" + str(i) for i in range(1000)]

    fpObj = open('data/wifiAPdata.json', 'w')

    while dt < end:

        for i in range(8):
            pickedSensor = sensors[random.randint(1, num - 1)]
            id = str(uuid.uuid1()),
            obs = {
                "id": id[0],
                "timeStamp": dt.strftime('%Y-%m-%d %H:%M:%S'),
                "sensor": pickedSensor,
                "payload": {
                    "clientId": clientIds[random.randint(1, 999)]
                },
                "type_": pickedSensor['type_']['observationType'],
            }
            fpObj.write(json.dumps(obs) + '\n')
        dt += step

    fpObj.close()
