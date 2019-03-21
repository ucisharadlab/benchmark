import random
import json
import uuid


def createSensors(numWifi, numWemo, numTemperature, src, dest):

    with open(src+'sensor.json') as data_file:
        sensors = json.load(data_file)

    with open(src+'user.json') as data_file:
        users = json.load(data_file)

    with open(src+'infrastructure.json') as data_file:
        rooms = json.load(data_file)

    wifiSensors = []
    wemoSensors = []
    temperatureSensors = []

    print ("Creating Sensors")

    for sensor in sensors:
        if sensor['type_']['id'] == "WiFiAP":
            wifiSensors.append(sensor)
        if sensor['type_']['id'] == "Thermometer":
            temperatureSensors.append(sensor)
        if sensor['type_']['id'] == "WeMo":
            wemoSensors.append(sensor)

    for i in range(numWifi-len(wifiSensors)):
        id = str(uuid.uuid4())
        copiedSensor = wifiSensors[random.randint(0, len(wifiSensors)-1)]
        copiedRoom = rooms[random.randint(0, len(rooms) - 1)]
        sensor = {
            "id": id.replace('-', '_'),
            "name": "simSensor{}".format(i),
            "coverage": copiedSensor['coverage'],
            "sensorConfig": copiedSensor['sensorConfig'],
            "type_": copiedSensor['type_'],
            "owner": copiedSensor['owner'],
            "infrastructure": copiedRoom
        }
        sensors.append(sensor)

    for i in range(numWemo-len(wemoSensors)):
        id = str(uuid.uuid4())
        copiedSensor = wemoSensors[random.randint(0, len(wemoSensors)-1)]
        owner = users[random.randint(0, len(users)-1)]
        copiedRoom = rooms[random.randint(0, len(rooms) - 1)]
        sensor = {
            "id": id.replace('-', '_'),
            "name": "simSensor{}".format(i),
            "coverage": copiedSensor['coverage'],
            "sensorConfig": copiedSensor['sensorConfig'],
            "type_": copiedSensor['type_'],
            "owner": owner,
            "infrastructure": copiedRoom
        }
        sensors.append(sensor)

    for i in range(numTemperature - len(temperatureSensors)):
        id = str(uuid.uuid4())
        copiedSensor = temperatureSensors[random.randint(0, len(temperatureSensors)-1)]
        owner = users[random.randint(0, len(users)-1)]
        copiedRoom = rooms[random.randint(0, len(rooms) - 1)]
        sensor = {
            "id": id.replace('-', '_'),
            "name": "simSensor{}".format(i),
            "coverage": copiedSensor['coverage'],
            "sensorConfig": copiedSensor['sensorConfig'],
            "type_": copiedSensor['type_'],
            "owner": owner,
            "infrastructure": copiedRoom
        }
        sensors.append(sensor)

    with open(dest + 'sensor.json', 'w') as writer:
        json.dump(sensors, writer, indent=4)


def createIntelligentSensors(numWemo, numTemperature, src, dest):

    with open(src+'sensor.json') as data_file:
        sensors = json.load(data_file)

    with open(src+'user.json') as data_file:
        users = json.load(data_file)

    wemoSensors = []
    temperatureSensors = []

    print ("Creating Sensors")

    for sensor in sensors:
        if sensor['type_']['id'] == "Thermometer":
            temperatureSensors.append(sensor)
        if sensor['type_']['id'] == "WeMo":
            wemoSensors.append(sensor)

    for wemo in wemoSensors:
        for i in range(numWemo/len(wemoSensors)):
            id = str(uuid.uuid4())
            copiedSensor = wemo
            owner = users[random.randint(0, len(users)-1)]
            sensor = {
                "id": id.replace('-', '_'),
                "name": "simSensor_{}_{}".format(copiedSensor['id'], i),
                "coverage": copiedSensor['coverage'],
                "sensorConfig": copiedSensor['sensorConfig'],
                "type_": copiedSensor['type_'],
                "owner": owner,
                "infrastructure": copiedSensor['infrastructure']
            }
            sensors.append(sensor)

    for tempSensor in temperatureSensors:
        for i in range(numTemperature/len(temperatureSensors)):
            id = str(uuid.uuid4())
            copiedSensor = tempSensor
            owner = users[random.randint(0, len(users)-1)]
            sensor = {
                "id": id.replace('-', '_'),
                "name": "simSensor_{}_{}".format(copiedSensor['id'], i),
                "coverage": copiedSensor['coverage'],
                "sensorConfig": copiedSensor['sensorConfig'],
                "type_": copiedSensor['type_'],
                "owner": owner,
                "infrastructure": copiedSensor['infrastructure']
            }
            sensors.append(sensor)

    with open(dest + 'sensor.json', 'w') as writer:
        json.dump(sensors, writer, indent=4)
