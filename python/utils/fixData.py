import json
import uuid
import random

src = "/home/benchmark/benchmark/benchmark/src/main/resources/data/"
dest = "/home/benchmark/benchmark/benchmark/data/"

def fixInfra():
    with open(src+'infrastructure.json') as data_file:
        infras = json.load(data_file)
    for infra in infras:
        infra['geometry'] = infra['region']['geometry']
        infra['floor'] = infra['region']['floor']
        del infra['region']

    with open(dest + 'infrastructure.json', 'w') as writer:
        json.dump(infras, writer, indent=4)


def createPlatforms():
    with open(src+'user.json') as data_file:
        users = json.load(data_file)
    with open(src+'platformType.json') as data_file:
        pts = json.load(data_file)

    i = 0
    platfoms = []
    for user in users:
        i += 1
        id = str(uuid.uuid4())
        platform = {
            "id": id,
            "name": "platform{}".format(i),
            "owner": user,
            "type_": pts[random.randint(0, len(pts) - 1)],
            "hashedMac": id
        }
        platfoms.append(platform)
    with open(dest + 'platform.json', 'w') as writer:
        json.dump(platfoms, writer, indent=4)


def fixPlatformsFromPlatform():
    with open(src+'platform.json') as data_file:
        platforms = json.load(data_file)

    for platform in platforms:
        if platform['type_']['name'] == "Laptop":
            platform['type_']['id'] = "pt3"

    with open(dest + 'platform.json', 'w') as writer:
        json.dump(platforms, writer, indent=4)


def fixSensorTypes():
    with open(src+'sensorType.json') as data_file:
        sts = json.load(data_file)
    with open(src+'observationType.json') as data_file:
        ots = json.load(data_file)

    for st in sts:
        for ot in ots:
            if st['observationTypeId'] == ot['id']:
                st['payloadSchema'] = ot['payloadSchema']
                del st['observationTypeId']
                break

    with open(dest + 'sensorType.json', 'w') as writer:
        json.dump(sts, writer, indent=4)


def fixSensors():
    with open(src+'sensor.json') as data_file:
        sensors = json.load(data_file)
    with open(src+'infrastructure.json') as data_file:
        infras = json.load(data_file)

    for sensor in sensors:
        for i in range(len(sensor['coverage'])):
            entity = sensor['coverage'][i]
            for infra in infras:
                if entity['id'] == infra['id']:
                    sensor['coverage'][i] = infra
                    break

    with open(dest + 'sensor.json', 'w') as writer:
        json.dump(sensors, writer, indent=4)


def readCoverage():
    coverageMap = {}
    with open("../data/coverageEstimation.txt") as f:
        for line in f:
            if line.startswith("3"):
                id, coverage = line.split("|")
                id = id.strip().replace('-', '_')
                coverage = coverage.strip().split(",")
                coverageMap[id] = coverage
    return coverageMap


def fixSensorCoverage():
    with open(src+'sensor.json') as data_file:
        data = json.load(data_file)

    sensors = []
    others = []

    for sensor in data:
        if sensor['type_']['id'] == "WiFiAP":
            sensors.append(sensor)
        else:
            others.append(sensor)

    with open(src+'infrastructure.json') as data_file:
        infras = json.load(data_file)

    coverageMap = readCoverage()
    for sensor in sensors:
        sensor['coverage'] = []
        for i in range(len(coverageMap[sensor['id']])):
            infraId = coverageMap[sensor['id']][i]
            for infra in infras:
                if infraId == infra['id']:
                    sensor['coverage'].append(infra)

    sensors.extend(others)

    with open(dest + 'sensor.json', 'w') as writer:
        json.dump(sensors, writer, indent=4)


def fixObservations():
    pass

fixSensorCoverage()