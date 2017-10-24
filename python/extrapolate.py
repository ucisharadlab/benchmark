import random
import json
import datetime
import uuid
from parser import Parser


SPEED_SCALED_FILE = "data/speedScaledObservation.json"
SENSOR_SCALED_FILE = "data/sensorScaledObservation.json"
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


class Scale(object):

    def __init__(self, dataDir, seedFile, outputFile, origDays, extendDays, origSpeed, extendSpeed, origSensor,
                 extendSensor, payloadName, speedScaleNoise, timeScaleNoise, deviceScaeNoise, type):
        self.outputFile = outputFile
        self.dataDir = dataDir
        self.seedFile = seedFile
        self.origDays = origDays
        self.origSpeed = origSpeed
        self.origSensor = origSensor
        self.extendDays = extendDays
        self.extendSpeed =extendSpeed
        self.extendSensor = extendSensor
        self.type = type
        if isinstance(payloadName, list):
            self.payloadName = payloadName[0]
            self.payloadList = payloadName
        else:
            self.payloadName = payloadName
            self.payloadList = None

        self.speedScaleNoise = speedScaleNoise
        self.timeScaleNoise = timeScaleNoise
        self.deviceScaleNoise = deviceScaeNoise
        self.writer = open(self.outputFile, "w")

        with open(self.dataDir + 'sensor.json') as data_file:
            data = json.load(data_file)
        self.sensorMap = {}
        for sensor in data:
            self.sensorMap[sensor['name']] = sensor

    def getRandAroundPayload(self, payload, scaleNoise):
        if self.type == int:
            min = (int)(payload * (1 - scaleNoise))
            max = (int)(payload * (1 + scaleNoise))
            return random.randint(0, max - min) + min
        elif self.type ==float:
            min = payload * (1 - scaleNoise)
            max = payload * (1 + scaleNoise)
            return round(min + (max-min)*random.random(), 2)

    def getRandBetweenPayloads(self, payload1,  payload2,  scaleNoise):
        if self.type == int:
            if payload1 < payload2:
                min = (int) (payload1 * (1 - scaleNoise))
                max = (int) (payload2 * (1 + scaleNoise))
                return random.randint(0, max - min) + min
            else:
                min = (int)(payload2 * (1 - scaleNoise))
                max = (int)(payload2 * (1 + scaleNoise))
                return random.randint(0, max - min) + min
        elif self.type == float:
            if payload1 < payload2:
                min = payload1 * (1 - scaleNoise)
                max = payload2 * (1 + scaleNoise)
            else:
                min = payload2 * (1 - scaleNoise)
                max = payload2 * (1 + scaleNoise)
            return round(random.random()*(max - min) + min, 2)

    def writeObject(self, timestamp, payload, sensor):
        try:
            if self.payloadList is not None:
                object = {
                "id": str(uuid.uuid4()),
                "sensor": sensor,
                "timeStamp": timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                "payload": {
                    self.payloadList[0]: payload,
                    self.payloadList[1]: random.randint(0, 100)
                }
            }
            else:
                object = {
                    "id": str(uuid.uuid4()),
                    "sensor": sensor,
                    "timeStamp": timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                    "payload": {
                        self.payloadName: payload
                    }
                }
            self.writer.write(json.dumps(object) + '\n')
        except Exception as e:
            print (e)
            print ("IO error")

    def speedScale(self):
        self.seedData = Parser(self.seedFile)

        self.writer = None
        try:
            self.writer = open(SPEED_SCALED_FILE, "w")
            prevObservation = self.seedData.getNext()
            prevTimestamp = datetime.datetime.strptime(prevObservation['timeStamp'], DATE_FORMAT)

            while prevObservation:

                self.writeObject(datetime.datetime.strptime(prevObservation['timeStamp'], DATE_FORMAT),
                                 prevObservation['payload'][self.payloadName],
                                 prevObservation['sensor'])

                currentObservaton = self.seedData.getNext()

                if currentObservaton is None:
                    prevObservation = currentObservaton
                    continue

                currentTimeStamp = datetime.datetime.strptime(currentObservaton['timeStamp'], DATE_FORMAT)

                if prevTimestamp > currentTimeStamp:
                    prevObservation = currentObservaton
                    continue

                prevPayload = prevObservation['payload'][self.payloadName]

                timestamp = prevTimestamp
                for i in range(self.origSpeed/self.extendSpeed - 1):
                    timestamp += datetime.timedelta(seconds=self.extendSpeed)
                    payload = self.getRandBetweenPayloads(prevPayload,
                                                          currentObservaton['payload'][self.payloadName],
                                                          self.speedScaleNoise)
                    self.writeObject(timestamp, payload, prevObservation['sensor'])
                    prevPayload = payload

                prevObservation = currentObservaton

        except KeyError as e:
            print("Speed IO error", e)
        finally :
            try:
                self.writer.close()
            except Exception as e:
                print("IO error")

    def getCopyOfSensor(self, sensor, numCopy):
        return self.sensorMap['simSensor_{}_{}'.format(sensor['id'], numCopy)]

    def deviceScale(self):
        self.seedData = Parser(SPEED_SCALED_FILE)

        self.writer = None
        try:
            self.writer = open(SENSOR_SCALED_FILE, "w")

            currentObservation = self.seedData.getNext()

            while currentObservation:
                self.writeObject(datetime.datetime.strptime(currentObservation['timeStamp'], DATE_FORMAT),
                                 currentObservation['payload'][self.payloadName],
                                 currentObservation['sensor'])
                for i in range(self.extendSensor/self.origSensor - 1):
                    payload = self.getRandAroundPayload(currentObservation['payload'][self.payloadName],
                                                          self.deviceScaleNoise)
                    self.writeObject(datetime.datetime.strptime(currentObservation['timeStamp'], DATE_FORMAT), payload,
                                     self.getCopyOfSensor(currentObservation['sensor'], i))

                currentObservation = self.seedData.getNext()

        except KeyError as e:
            print("Device IO error", e)
        finally:
            try:
                self.writer.close()
            except Exception as e:
                print("IO error")

    def timeScale(self):
        self.seedData = Parser(SENSOR_SCALED_FILE)

        self.writer = None
        try:
            self.writer = open(self.outputFile, "w")

            currentObservation = self.seedData.getNext()

            while currentObservation:
                self.writeObject(datetime.datetime.strptime(currentObservation['timeStamp'], DATE_FORMAT),
                                 currentObservation['payload'][self.payloadName],
                                 currentObservation['sensor'])
                timestamp = datetime.datetime.strptime(currentObservation['timeStamp'], DATE_FORMAT)
                for i in range(1, self.extendDays/self.origDays):
                    payload = self.getRandAroundPayload(currentObservation['payload'][self.payloadName],
                                                          self.deviceScaleNoise)
                    timestamp += datetime.timedelta(days=i*self.origDays)
                    self.writeObject(timestamp, payload, currentObservation['sensor'])

                currentObservation = self.seedData.getNext()

        except KeyError as e:
            print("Time IO error", e)
        finally:
            try:
                self.writer.close()
            except Exception as e:
                print("IO error")


class SemanticScale(object):
    def __init__(self, dataDir, seedFile, outputFile, origDays, extendDays, origSpeed, extendSpeed,
                 payloadName, speedScaleNoise, timeScaleNoise, type):
        self.outputFile = outputFile
        self.dataDir = dataDir
        self.seedFile = seedFile
        self.origDays = origDays
        self.origSpeed = origSpeed
        self.extendDays = extendDays
        self.extendSpeed = extendSpeed
        self.type = type
        self.payloadName = payloadName
        self.speedScaleNoise = speedScaleNoise
        self.timeScaleNoise = timeScaleNoise
        self.writer = open(self.outputFile, "w")

    def getRandAroundPayload(self, payload, scaleNoise):
        if self.type == int:
            min = (int)(payload * (1 - scaleNoise))
            max = (int)(payload * (1 + scaleNoise))
            return random.randint(0, max - min) + min
        elif self.type ==float:
            min = payload * (1 - scaleNoise)
            max = payload * (1 + scaleNoise)
            return min + (max-min)*random.random()

    def getRandBetweenPayloads(self, payload1,  payload2,  scaleNoise):
        if self.type == int:
            if payload1 < payload2:
                min = (int) (payload1 * (1 - scaleNoise))
                max = (int) (payload2 * (1 + scaleNoise))
                return random.randint(0, max - min) + min
            else:
                min = (int)(payload2 * (1 - scaleNoise))
                max = (int)(payload2 * (1 + scaleNoise))
                return random.randint(0, max - min) + min
        elif self.type == float:
            if payload1 < payload2:
                min = payload1 * (1 - scaleNoise)
                max = payload2 * (1 + scaleNoise)
                return random.random()*(max - min) + min
            else:
                min = payload2 * (1 - scaleNoise)
                max = payload2 * (1 + scaleNoise)
                return random.random()*(max - min) + min

    def writeObject(self, timestamp, payload, sensor, entity):
        try:
            object = {
                "id": str(uuid.uuid4()),
                "semanticEntity": entity,
                "virtualSensor": sensor,
                "type_": sensor['type_']['semanticObservationType'],
                "timeStamp": timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                "payload": {
                    self.payloadName: payload
                }
            }
            self.writer.write(json.dumps(object) + '\n')
        except Exception as e:
            print (e)
            print ("IO error")

    def speedScale(self):
        self.seedData = Parser(self.seedFile)

        self.writer = None
        try:
            self.writer = open(SPEED_SCALED_FILE, "w")
            prevObservation = self.seedData.getNext()
            prevTimestamp = datetime.datetime.strptime(prevObservation['timeStamp'], DATE_FORMAT)

            while prevObservation:

                self.writeObject(datetime.datetime.strptime(prevObservation['timeStamp'], DATE_FORMAT),
                                 prevObservation['payload'][self.payloadName],
                                 prevObservation['virtualSensor'], prevObservation['semanticEntity'])

                currentObservaton = self.seedData.getNext()

                if currentObservaton is None:
                    prevObservation = currentObservaton
                    continue

                currentTimeStamp = datetime.datetime.strptime(currentObservaton['timeStamp'], DATE_FORMAT)

                if prevTimestamp > currentTimeStamp:
                    prevObservation = currentObservaton
                    continue

                prevPayload = prevObservation['payload'][self.payloadName]

                timestamp = prevTimestamp
                for i in range(self.extendSpeed / self.origSpeed - 1):
                    timestamp += datetime.timedelta(seconds=self.extendSpeed)
                    payload = self.getRandBetweenPayloads(prevPayload,
                                                          currentObservaton['payload'][self.payloadName],
                                                          self.speedScaleNoise)
                    self.writeObject(timestamp, payload, prevObservation['virtualSensor'], prevObservation['semanticEntity'])
                    prevPayload = payload

                prevObservation = currentObservaton

        except KeyError as e:
            print("Speed IO error", e)
        finally:
            try:
                self.writer.close()
            except Exception as e:
                print("IO error")

    def timeScale(self):
        self.seedData = Parser(SPEED_SCALED_FILE)

        self.writer = None
        try:
            self.writer = open(self.outputFile, "w")

            currentObservation = self.seedData.getNext()

            while currentObservation:
                self.writeObject(datetime.datetime.strptime(currentObservation['timeStamp'], DATE_FORMAT),
                                 currentObservation['payload'][self.payloadName],
                                 currentObservation['virtualSensor'], currentObservation['semanticEntity'])
                timestamp = datetime.datetime.strptime(currentObservation['timeStamp'], DATE_FORMAT)
                for i in range(1, self.extendDays / self.origDays):
                    payload = self.getRandAroundPayload(currentObservation['payload'][self.payloadName],
                                                        self.timeScaleNoise)
                    timestamp += datetime.timedelta(days=i * self.origDays)
                    self.writeObject(timestamp, payload, currentObservation['virtualSensor'], currentObservation['semanticEntity'])

                currentObservation = self.seedData.getNext()

        except KeyError as e:
            print("Time IO error", e)
        finally:
            try:
                self.writer.close()
            except Exception as e:
                print("IO error")