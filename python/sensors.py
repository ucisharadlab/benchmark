import random
import json
import uuid


def createSensors(numWifi, numWemo, numTemperature, src, dest):

    with open(src+'sensor.json') as data_file:
        sensors = json.load(data_file)

    with open(src+'user.json') as data_file:
        users = json.load(data_file)

    wifiSensors = []
    wemoSensors = []
    temperatureSensors = []

    for sensor in sensors:
        if sensor['type_']['id'] == "WiFiAP":
            wifiSensors.append(sensor)
        if sensor['type_']['id'] == "EnergyMeter":
            temperatureSensors.append(sensor)
        if sensor['type_']['id'] == "WeMo":
            wemoSensors.append(sensor)

    for i in range(numWifi):
        id = str(uuid.uuid1()),
        copiedSensor = wifiSensors[random.randint(0, len(wifiSensors)-1)]
        sensor = {
            "id": id[0],
            "name": "simSensor{}".format(i),
            "coverage": copiedSensor['coverage'],
            "sensorConfig": copiedSensor['sensorConfig'],
            "type_": copiedSensor['type_'],
            "owner": copiedSensor['owner'],
            "infrastructure": copiedSensor['infrastructure']
        }
        sensors.append(sensor)

    for i in range(numWemo):
        id = str(uuid.uuid1()),
        copiedSensor = wemoSensors[random.randint(0, len(wemoSensors)-1)]
        owner = users[random.randint(0, len(users)-1)]
        sensor = {
            "id": id[0],
            "name": "simSensor{}".format(i),
            "coverage": copiedSensor['coverage'],
            "sensorConfig": copiedSensor['sensorConfig'],
            "type_": copiedSensor['type_'],
            "owner": owner,
            "infrastructure": copiedSensor['infrastructure']
        }
        sensors.append(sensor)

    for i in range(numTemperature):
        id = str(uuid.uuid1()),
        copiedSensor = temperatureSensors[random.randint(0, len(temperatureSensors)-1)]
        owner = users[random.randint(0, len(users)-1)]
        sensor = {
            "id": id[0],
            "name": "simSensor{}".format(i),
            "coverage": copiedSensor['coverage'],
            "sensorConfig": copiedSensor['sensorConfig'],
            "type_": copiedSensor['type_'],
            "owner": owner,
            "infrastructure": copiedSensor['infrastructure']
        }
        sensors.append(sensor)

    with open(dest + 'sensor.json', 'w') as writer:
        json.dump(sensors, writer, indent=4)
