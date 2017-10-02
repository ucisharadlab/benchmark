import datetime
import random
import json
import uuid

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
    if sensor['sensorType']['id'] == "WiFiAP":
        sensors.append(sensor)

num = len(sensors)

clientIds = ["client_" + str(i) for i in range(1000)]

while dt < end:

    for i in range(8):
        pickedSensor = sensors[random.randint(1, num - 1)]

        obs = {
	    "id": str(uuid.uuid1()),
            "timeStamp": dt.strftime('%Y-%m-%d %H:%M:%S'),
            "sensorId": pickedSensor['id'],
            "payload": {
                "clientId": clientIds[random.randint(1, 999)]
            },
            "typeId": pickedSensor['sensorType']['observationType']['id'],
        }
        obsRowList.append(obs)

        obs = {
            "timeStamp": dt.strftime('%Y-%m-%d %H:%M:%S'),
            "sensor": pickedSensor,
            "payload": {
                "clientId": clientIds[random.randint(1, 999)]
            },
            "type_": pickedSensor['sensorType']['observationType'],
        }
        obsObjectList.append(obs)

    dt += step

with open(ROWS + 'wifiAPdata.json', 'w') as fp:
    json.dump(obsRowList, fp)

with open(OBJECTS + 'wifiAPdata.json', 'w') as fp:
    json.dump(obsObjectList, fp)
