import random
import json

NUM_NEIGHBOURS = 3

def createWifiMap(dataDir, file):

    with open(dataDir + 'sensor.json') as data_file:
        data = json.load(data_file)

    sensors = []

    for sensor in data:
        if sensor['type_']['id'] == "WiFiAP":
            sensors.append(sensor)

    floorMap = {i: [] for i in range(1, 7)}

    for sensor in sensors:
        floorMap[sensor['infrastructure']['floor']].append(sensor['id'])

    wifiMap = {sensor['id']:[] for sensor in sensors}

    for sensor in sensors:
        wifiOnSameFloor = [sensorId for sensorId in floorMap[sensor['infrastructure']['floor']] if sensor['id'] != sensorId ]
        random.shuffle(wifiOnSameFloor)
        wifiMap[sensor['id']] = wifiOnSameFloor[:NUM_NEIGHBOURS]
        wifiMap[sensor['id']].append(sensor['id'])

    for i in range(1, 7):
        sensorId = floorMap[i][random.randint(0, len(floorMap[i])-1)]
        if i < 6:
            wifiMap[sensorId].append(floorMap[i + 1][random.randint(0, len(floorMap[i + 1]) - 1)])
        if i > 1:
            wifiMap[sensorId].append(floorMap[i - 1][random.randint(0, len(floorMap[i - 1]) - 1)])

    with open(file, 'w') as writer:
        json.dump(wifiMap, writer, indent=4)

createWifiMap("../../data/", "../../data/wifiMap.json")