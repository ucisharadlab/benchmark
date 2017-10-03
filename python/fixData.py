import json
import uuid
import random

src = "/home/peeyush/benchmark/benchmark/src/main/resources/data/"
dest = "/home/peeyush/benchmark/benchmark/data/"

def fixInfra():
    with open(src+'infrastructure.json') as data_file:
        infras = json.load(data_file)
    for infra in infras:
        infra['geometry'] = infra['region']['geometry']
        infra['floor'] = infra['region']['floor']
        del infra['region']

    with open(dest + 'infrastructure.json', 'w') as writer:
        json.dump(infras, writer, indent=4)


def fixPlatforms():
    with open(src+'user.json') as data_file:
        users = json.load(data_file)
    with open(src+'platformType.json') as data_file:
        pts = json.load(data_file)

    i = 0
    platfoms = []
    for user in users:
        i += 1
        id = str(uuid.uuid1())
        platform = {
            "id": id,
            "name": "platform{}".format(i),
            "owner": users[random.randint(1, len(users) - 1)],
            "type_": pts[random.randint(1, len(pts) - 1)],
            "hashedMac": id
        }
        platfoms.append(platform)
        with open(dest + 'platform.json', 'w') as writer:
            json.dump(platfoms, writer, indent=4)


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

def fixObservations():
    pass