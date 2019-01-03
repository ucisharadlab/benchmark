import uuid
import json
import random
import datetime


class TrajectoryScale(object):

    def __init__(self, dataDir, presenceOutFile, wifiOutFile, wifiMapFile, startTime, days, speed):
        self.presenceWriter = open(presenceOutFile, "w")
        self.wifiWriter = open(wifiOutFile, "w")
        self.dataDir = dataDir
        self.wifiMapFile = wifiMapFile
        self.startTime = startTime
        self.days = days
        self.speed = speed
        self.initialize()

    def initialize(self):
        self.clientMap = {}
        with open(self.dataDir + 'platform.json') as data_file:
            data = json.load(data_file)
        for platform in data:
            self.clientMap[platform['owner']['id']] = platform['hashedMac']

        with open(self.dataDir + 'user.json') as data_file:
            self.users = json.load(data_file)

        with open(self.dataDir + 'sensor.json') as data_file:
            data = json.load(data_file)
        self.wifiSensors = []
        self.wifiSensorMap = {}
        for sensor in data:
            if sensor['type_']['id'] == "WiFiAP":
                self.wifiSensors.append(sensor)
                self.wifiSensorMap[sensor['id']] = sensor

        self.virtualSensor = None
        with open(self.dataDir + 'virtualSensor.json') as data_file:
            vs = json.load(data_file)
        for v in vs:
            if v['type_']['id'] == "WiFiToPresence":
                self.virtualSensor = v
                break

        with open(self.dataDir + 'wifiMap.json') as data_file:
            self.wifiMap = json.load(data_file)

    def writeWiFiObject(self, timestamp, clientId, sensor):
        try:
            object = {
                "id": str(uuid.uuid4()),
                "sensor": sensor,
                "timeStamp": timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                "payload": {
                    "clientId": clientId
                }
            }
            self.wifiWriter.write(json.dumps(object) + '\n')
        except Exception as e:
            print (e)
            print ("IO error")

    def writePresenceObject(self, timestamp, location, user):
        try:
            object = {
                "id": str(uuid.uuid4()),
                "semanticEntity": user,
                "virtualSensor": self.virtualSensor,
                "type_": self.virtualSensor["type_"]["semanticObservationType"],
                "timeStamp": timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                "payload": {
                    "location": location
                }
            }
            self.presenceWriter.write(json.dumps(object) + '\n')
        except Exception as e:
            print (e)
            print ("IO error")

    def getSensorById(self, id):
        return self.wifiSensorMap[id]

    def getClientIdByUser(self, user):
        return self.clientMap[user['id']]

    def generatePersonPaths(self):

        try:
            for i in range(len(self.users)):
                user = self.users[i]
                wifi = self.wifiSensors[random.randint(0, len(self.wifiSensors)-1)]
                timestamp = self.startTime

                while timestamp < self.startTime + datetime.timedelta(days=self.days):

                    self.writeWiFiObject(timestamp, self.getClientIdByUser(user), wifi)
                    self.writePresenceObject(timestamp, wifi['infrastructure']['id'], user)

                    neighbours = self.wifiMap[wifi['id']]
                    wifi = self.getSensorById(neighbours[random.randint(0, len(neighbours)-1)])

                    timestamp += datetime.timedelta(seconds=self.speed)

        except Exception as e:
            print("IO error", e)
        finally:
            try:
                self.presenceWriter.close()
                self.wifiWriter.close()
            except Exception as e:
                print("IO error")
