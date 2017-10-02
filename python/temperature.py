import datetime
import random
import json
import uuid

# dt = datetime.datetime(2017, 7, 11, 0, 0, 0)
# end = datetime.datetime(2017, 9, 11, 23, 59, 59)
# step = datetime.timedelta(seconds=500)

ROWS = "data/rows/"
OBJECTS = "data/objects/"


def createTemperaturebservations(dt, end, step):

    with open('../src/main/resources/data/rows/sensor.json') as data_file:
        data = json.load(data_file)

    sensors = []

    for sensor in data:
        if sensor['sensorType']['id'] == "EnergyMeter":
            sensors.append(sensor)
    num = len(sensors)

    fpRow = open(ROWS + 'temperatureData.json', 'w')
    fpObj = open(OBJECTS + 'temperatureData.json', 'w')

    while dt < end:

        for i in range(8):
            pickedSensor = sensors[random.randint(1, num - 1)]
            id = str(uuid.uuid1()),

            obs = {
                "id": id[0],
                "timeStamp": dt.strftime('%Y-%m-%d %H:%M:%S'),
                "sensorId": pickedSensor['id'],
                "payload": {
                    "temperature": random.randint(1, 100)
                },
                "typeId": "EnergyMeterType",
            }
            fpRow.write(json.dumps(obs) +"\n")

            obs = {
                "id": id[0],
                "timeStamp": dt.strftime('%Y-%m-%d %H:%M:%S'),
                "sensor": pickedSensor,
                "payload": {
                    "temperature": random.randint(1, 100)
                },
                "type_": pickedSensor['sensorType']['observationType'],
            }
            fpObj.write(json.dumps(obs) + '\n')

        dt += step

    fpRow.close()
    fpObj.close()