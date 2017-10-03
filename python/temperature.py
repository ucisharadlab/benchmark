import datetime
import random
import json
import uuid


def createTemperaturebservations(dt, end, step, dataDir):

    with open(dataDir + 'sensor.json') as data_file:
        data = json.load(data_file)

    sensors = []

    for sensor in data:
        if sensor['type_']['id'] == "EnergyMeter":
            sensors.append(sensor)
    num = len(sensors)

    fpObj = open('data/temperatureData.json', 'w')

    while dt < end:

        for i in range(8):
            pickedSensor = sensors[random.randint(1, num - 1)]
            id = str(uuid.uuid1()),
            obs = {
                "id": id[0],
                "timeStamp": dt.strftime('%Y-%m-%d %H:%M:%S'),
                "sensor": pickedSensor,
                "payload": {
                    "temperature": random.randint(1, 100)
                }
            }
            fpObj.write(json.dumps(obs) + '\n')

        dt += step

    fpObj.close()