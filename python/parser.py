import json


class Parser(object):

    def __init__(self, dataDir, seedFile):

        with open(seedFile) as data_file:
            data = json.load(data_file)

        sensors = []

        for sensor in data:
            if sensor['type_']['id'] == "WeMo":
                sensors.append(sensor)
        num = len(sensors)

    def getSensorIds(self):
        pass

    def getPayloadValues(self):
        pass

    def getTimestamps(self):
        pass

    def getOrigDays(self):
        pass

    def getOrigSpeed(self):
        pass

    def getPayloadName(self):
        pass