import random
import json
from parser import Parser

SPEED_SCALED_FILE = "data/speedScaledObservation.txt"
SENSOR_SCALED_FILE = "data/sensorScaledObservation.txt"


class CounterScale(object):

    def __init__(self, dataDir, seedFile, outputFile, days, speed, sensors, type):
        self.outputFile = outputFile
        self.dataDir = dataDir
        self.seedFile = seedFile
        self.days = days
        self.speed = speed
        self.sensors = sensors
        self.type = type
        self.writer = open(self.outputFile, "w")

    def getRandAroundPayload(self, payload, scaleNoise):
        min = (int)(payload * (1 - scaleNoise))
        max = (int)(payload * (1 + scaleNoise))
        return random.randint(0, max - min) + min

    def getRandBetweenPayloads(self, payload1,  payload2,  scaleNoise):
        if payload1 < payload2:
            min = (int) (payload1 * (1 - scaleNoise))
            max = (int) (payload2 * (1 + scaleNoise))
            return random.randint(0, max - min) + min
        else:
            min = (int)(payload2 * (1 - scaleNoise))
            max = (int)(payload2 * (1 + scaleNoise))
            return random.randint(0, max - min) + min

    def writeObject(self, timestamp, payload, sensorId, payloadName):
        try:
            object = {
                "sensorId": sensorId,
                "timeStamp": timestamp,
                "payload": {
                    payloadName: payload
                }
            }
            self.writer.write(json.dumps(object) + '\n')
        except Exception as e:
            print (e)
            print ("IO error")

    def speedScale(self, speedScaleNum, speedScaleNoise):
        self.seedData = Parser(self.dataDir, self.seedFile)
        sensorIds = self.seedData.getSensorIds()
        payloadValues = self.seedData.getPayloadValues()
        timestamps = self.seedData.getTimestamps()
        origSpeed = self.seedData.getOrigSpeed()
        origDays = self.seedData.getOrigDays()
        payloadName = self.seedData.getPayloadName()
        numSensors = len(sensorIds)

        self.writer = None
        try:
            self.writer = open(SPEED_SCALED_FILE, "w")

            count = 1
            for m in range(origDays):
                timestamp = helper.timeAddDays(timestamps.get(0), m)
                for i in range(obsSpeed):
                    for j in range(sensorSize):
                        payload = payloads.get(j+i * sensorSize)
                        self.writeObject(timestamp, payload, sensorIds.get(j), payloadName)
                        count += 1
                        print("SpeedScale" + count)

                    timestamp = helper.increaseTime(timestamp, scaleSpeed)

                    for k in range(speedScaleNum-1):
                        for j in range(sensorSize):
                            payload = self.getRandBetweenPayloads(payloads.get(i * sensorSize+j),
                                                                  payloads.get(i * sensorSize+j+sensorSize), speedScaleNoise)
                            self.writeObject(timestamp, payload, sensorIds.get(j), payloadName)
                            count += 1
                            print("SpeedScale" + count)

                        timestamp = helper.increaseTime(timestamp, scaleSpeed)

                for j in range(numSensors):
                    payload = payloadValues.get((origSpeed-1) * numSensors + j)
                    self.writeObject(timestamp, payload, sensorIds.get(j), payloadName)
                    count += 1
                    print("SpeedScale" + count)

                for k in range(speedScaleNum-1):
                    timestamp += scaleSpeed
                    for j in range(numSensors):
                        payload = self.getRandAroundPayload(payloadValues.get((origSpeed-1) * numSensors + j), speedScaleNoise)
                        self.writeObject(timestamp, payload, sensorIds.get(j), payloadName)
                        count += 1
                        print("SpeedScale" + count)

        except Exception as e:
            print("IO error")
        finally :
            try:
                self.writer.close()
            except Exception as e:
                print("IO error")

    def deviceScale(self, scaleNum, deviceScaleNoise, simulatedName):
        self.seedData = Parser(self.dataDir, SPEED_SCALED_FILE)
        sensorIds = self.seedData.getSensorIds()
        payloadValues = self.seedData.getPayloadValues()
        timestamps = self.seedData.getTimestamps()
        origSpeed = self.seedData.getOrigSpeed()
        origDays = self.seedData.getOrigDays()
        payloadName = self.seedData.getPayloadName()
        numSensors = len(sensorIds)

        sensorSize = sensorIds.size()

        scaledSensorSize = sensorSize * scaleNum
        sensorIds = helper.scaleSensorIds(sensorIds, scaleNum, simulatedName)

        self.writer = None
        try:
            self.writer = open(SENSOR_SCALED_FILE, "w")

            count = 1
            for m in range(recordDays):
                pastObs = m * obsSpeed * sensorSize
                for i in range(obsSpeed):
                    timestamp = timestamps.get(m * obsSpeed+i)

                for j in range(scaledSensorSize):
                    if j < sensorSize:
                        payload = payloads.get(pastObs+i * sensorSize+j)
                    else:
                        n = random.randint(sensorSize-1)
                        payload = self.getRandAroundPayload(payloads.get(pastObs+i * sensorSize+n), deviceScaleNoise)

                    self.writeObject(timestamp, payload, sensorIds.get(j), payloadName)

                    count += 1
                    print("DeviceScale" + count)

        except Exception as e:
            print("IO error")
        finally:
            try:
                self.writer.close()
            except Exception as e:
                print("IO error")

    def timeScale(self, timeScaleNoise, extendDays):
        self.seedData = Parser(self.dataDir, SENSOR_SCALED_FILE)
        sensorIds = self.seedData.getSensorIds()
        payloadValues = self.seedData.getPayloadValues()
        timestamps = self.seedData.getTimestamps()
        origSpeed = self.seedData.getOrigSpeed()
        origDays = self.seedData.getOrigDays()
        payloadName = self.seedData.getPayloadName()
        numSensors = len(sensorIds)

        self.writer = None
        try:
            self.writer = open(self.outputFilename, "w")
            count = 1

            for m in range(origDays):
                pastObs = m * origSpeed * numSensors
                for i in range(origSpeed):
                    timestamp = timestamps[i]
                    for j in range(numSensors):
                        payload = payloadValues[pastObs + i*numSensors + j]
                        self.writeObject(timestamp, payload, sensorIds[j], payloadName)
                        count += 1

            for m in range(extendDays):
                pastDays = self.days + m
                for i in range(obsSpeed):
                    timestamp = helper.timeAddDays(timestamps.get(i), pastDays)
                    for j in range(sensorSize):
                        payload = self.getRandAroundPayload(payloads.get(i * sensorSize+j), timeScaleNoise)
                        self.writeObject(timestamp, payload, sensorIds.get(j), payloadName)
                        count += 1
                        print("TimeScale" + count)

            self.writer.close()

        except Exception as e:
            print("IO error")
        finally:
            try:
                self.writer.close()
            except Exception as e:
                print("IO error")



