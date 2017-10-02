import datetime
import random
import json

dt = datetime.datetime(2017, 7, 11, 0, 0, 0)
end = datetime.datetime(2017, 9, 11, 23, 59, 59)
step = datetime.timedelta(seconds=500)

ROWS = "data/rows/"
OBJECTS = "data/objects/"
MIX = "data/mix/"

obsRowList = []
obsObjectList = []
obsMixList = []

with open('../src/main/resources/data/rows/sensor.json') as data_file:
    data = json.load(data_file)

sensors = []

for sensor in data:
    if sensor['sensorType']['id'] == "EnergyMeter":
        sensors.append(sensor)
num = len(sensors)

while dt < end:

    for i in range(8):
        pickedSensor = sensors[random.randint(1, num - 1)]
        obs = {
            "timestamp": dt.strftime('%Y-%m-%d %H:%M:%S'),
            "sensorId": pickedSensor['id'],
            "payload": {
                "temperature": random.randint(1, 100)
            },
            "typeId": "EnergyMeterType",
        }
        obsRowList.append(obs)

        obs = {
            "timestamp": dt.strftime('%Y-%m-%d %H:%M:%S'),
            "sensor": pickedSensor,
            "payload": {
                "temperature": random.randint(1, 100)
            },
            "type_": pickedSensor['sensorType']['observationType'],
        }
        obsObjectList.append(obs)

    dt += step

with open(ROWS + 'temperatureData.json', 'w') as fp:
    json.dump(obsRowList, fp)

with open(OBJECTS + 'temperatureData.json', 'w') as fp:
    json.dump(obsObjectList, fp)
