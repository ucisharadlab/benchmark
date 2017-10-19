from helper import Helper
import helper
import random
import json


class CounterScale(object):

    def __init__(self, seedData, outputFile, days, speed, sensors):
        self.outputFilename = outputFile
        self.seedData = seedData
        self.days = days
        self.speed = speed
        self.sensors = sensors
        self.helper = Helper()

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

    def helpWriteToFile(self, jsonWriter, timestamp, payload, sensorId, payloadName):
        try:
            object = {
                "sensorId": sensorId,
                "timeStamp": timestamp,
                "payload": {
                    payloadName: payload
                }
            }
            jsonWriter.write(json.dumps(object) + '\n')
        except Exception as e:
            print (e)
            print ("IO error")

        return jsonWriter

    def timeScale(self, timeScaleNoise, extendDays):
        sensorTypeIds = counterObs.getTypeIds()
        sensorType = sensorTypeIds.get(0)
        sensorIds = counterObs.getSensorIds()
        payloads = counterObs.getPayloads()
        timestamps = counterObs.getTimestamps()
        obsSpeed = counterObs.getObsSpeed()
        recordDays = counterObs.getRecordDays()
        payloadName = counterObs.getPayloadName()
        sensorSize = sensorIds.size()

        jsonWriter = None
        try:
            jsonWriter = open(self.outputFilename, "w+")
            count = 1

            for m in range(recordDays):
                pastObs = m * obsSpeed * sensorSize
                for i in range(obsSpeed):
                    timestamp = timestamps.get(i)
                    for j in range(sensorSize):
                        payload = payloads.get(pastObs+i * sensorSize+j)
                        self.helpWriteToFile(jsonWriter, timestamp, payload, sensorIds.get(j), payloadName)
                        count += 1

            for m in range(extendDays):
                pastDays = recordDays + m
                for i in range(obsSpeed):
                    timestamp = helper.timeAddDays(timestamps.get(i), pastDays)
                    for j in range(sensorSize):
                        payload = self.getRandAroundPayload(payloads.get(i * sensorSize+j), timeScaleNoise)
                        self.helpWriteToFile(jsonWriter, timestamp, payload, sensorIds.get(j), payloadName)
                        count += 1
                        print("TimeScale" + count)

            jsonWriter.close()

        except Exception as e:
            print("IO error")
        finally:
            try:
                jsonWriter.close()
            except Exception as e:
                print("IO error")

    def speedScale(self, speedScaleNum, speedScaleNoise):
        counterObs = CounterObsParser()
        counterObs.parseData(self.outputFilename)
        sensorTypeIds = counterObs.getTypeIds()
        sensorType = sensorTypeIds.get(0)
        sensorIds = counterObs.getSensorIds()
        payloads = counterObs.getPayloads()
        timestamps = counterObs.getTimestamps()
        recordDays = counterObs.getRecordDays()
        payloadName = counterObs.getPayloadName()
        obsSpeed = counterObs.getObsSpeed()
        sensorSize = sensorIds.size()
        scaleSpeed = obsSpeed * speedScaleNum

        jsonWriter = None
        try:
            jsonWriter = open(self.outputFilename, "w+")

            count = 1
            for m in range(recordDays):
                timestamp = helper.timeAddDays(timestamps.get(0), m)
                for i in range(obsSpeed):
                    for j in range(sensorSize):
                        payload = payloads.get(j+i * sensorSize)
                        self.helpWriteToFile(jsonWriter, timestamp, payload, sensorIds.get(j), payloadName)
                        count += 1
                        print("SpeedScale" + count)

                    timestamp = helper.increaseTime(timestamp, scaleSpeed)

                    for k in range(speedScaleNum-1):
                        for j in range(sensorSize):
                            payload = self.getRandBetweenPayloads(payloads.get(i * sensorSize+j),
                                                                  payloads.get(i * sensorSize+j+sensorSize), speedScaleNoise)
                            self.helpWriteToFile(jsonWriter, timestamp, payload, sensorIds.get(j), payloadName)
                            count += 1
                            print("SpeedScale" + count)

                        timestamp = helper.increaseTime(timestamp, scaleSpeed)

                for j in range(sensorSize):
                    payload = payloads.get((obsSpeed-1) * sensorSize + j)
                    self.helpWriteToFile(jsonWriter, timestamp, payload, sensorIds.get(j), payloadName)
                    count += 1
                    print("SpeedScale" + count)

                for k in range(speedScaleNum-1):
                    timestamp = helper.increaseTime(timestamp, scaleSpeed)
                    for j in range(sensorSize):
                        payload = self.getRandAroundPayload(payloads.get((obsSpeed-1) * sensorSize + j), speedScaleNoise)
                        self.helpWriteToFile(jsonWriter, timestamp, payload, sensorIds.get(j), payloadName)
                        count += 1
                        print("SpeedScale" + count)

        except Exception as e:
            print("IO error")
        finally :
            try:
                jsonWriter.close()
            except Exception as e:
                print("IO error")

    def deviceScale(self, scaleNum, deviceScaleNoise, simulatedName):
        counterObs = CounterObsParser()
        counterObs.parseData(self.outputFilename)
        sensorTypeIds = counterObs.getTypeIds()
        sensorType = sensorTypeIds.get(0)
        sensorIds = counterObs.getSensorIds()
        payloads = counterObs.getPayloads()
        timestamps = counterObs.getTimestamps()
        recordDays = counterObs.getRecordDays()
        payloadName = counterObs.getPayloadName()
        obsSpeed = counterObs.getObsSpeed()

        sensorSize = sensorIds.size()

        scaledSensorSize = sensorSize * scaleNum
        sensorIds = helper.scaleSensorIds(sensorIds, scaleNum, simulatedName)

        jsonWriter = None
        try:
            jsonWriter = open(self.outputFilename, "w+")

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

                    self.helpWriteToFile(jsonWriter, timestamp, payload, sensorIds.get(j), payloadName)

                    count += 1
                    print("DeviceScale" + count)

        except Exception as e:
            print("IO error")
        finally:
            try:
                jsonWriter.close()
            except Exception as e:
                print("IO error")