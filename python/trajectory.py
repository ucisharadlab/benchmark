from parser import TrajectoryParser
import uuid
import json
import random
import datetime


class TrajectoryScale(object):

    def __init__(self, dataDir, presenceOutFile, wifiOutFile, wifiMapFile, numUsers, startTime, days, speed):
        self.presenceWriter = open(presenceOutFile, "w")
        self.wifiWriter = open(wifiOutFile, "w")
        self.dataDir = dataDir
        self.wifiMapFile = wifiMapFile
        self.numUsers = numUsers
        self.startTime = startTime
        self.days = days
        self.speed = speed

    def writeWiFiObject(self, timestamp, payload, sensor):
        try:
            object = {
                "id": str(uuid.uuid4()),
                "sensor": sensor,
                "timeStamp": timestamp,
                "payload": {
                    "location": payload
                }
            }
            self.wifiWriter.write(json.dumps(object) + '\n')
        except Exception as e:
            print (e)
            print ("IO error")

    def writePresenceObject(self, timestamp, payload, user):
        try:
            object = {
                "id": str(uuid.uuid4()),
                "semanticEntity": user,
                "virtualSensor": self.virtualSensor,
                "type_": self.presenceType,
                "timeStamp": timestamp,
                "payload": {
                    "location": payload
                }
            }
            self.presenceWriter.write(json.dumps(object) + '\n')
        except Exception as e:
            print (e)
            print ("IO error")

    def getSensorById(self):
        return None

    def generatePersonPaths(self):

        traj = TrajectoryParser()
        traj.parseData(self.outputFilename)
        connectMap = traj.getConnectMap()

        keys = connectMap.keySet()
        keyArray = keys.toArray(String[keys.size()])
        keySize = keyArray.length

        try:
            for i in range(1, self.numUsers+1):
                keyIndex = random.randint(0, keySize)
                area = keyArray[keyIndex]
                timestamp = self.startTime

                while timestamp < timestamp + datetime.timedelta(days=self.days):

                    jsonWriter.beginObject()
                    jsonWriter.name("area")
                    jsonWriter.value(area)
                    jsonWriter.name("timestamp")
                    jsonWriter.value(timestamp)
                    jsonWriter.endObject()

                    connectsOfArea = connectMap.get(area)
                    connectsOfArea.add(area)
                    connectSize = connectsOfArea.size()
                    area = connectsOfArea.get(random.randint(connectSize))

                    timestamp += datetime.timedelta(seconds=self.speed)

        except Exception as e:
            print("IO error")
        finally:
            try:
                self.presenceWriter.close()
                self.wifiWriter.close()
            except Exception as e:
                print("IO error")
