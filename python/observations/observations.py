import json
import uuid

import temperature
import wemo
import wifiap


def createObservations(dt, end, step, dataDir, outputDir):

    line = None
    finalObj = open(outputDir + 'observation.json', 'w')
    finalObj.write("[\n")

    with open(dataDir+'observation.json') as data_file:
        seedObservations = json.load(data_file)

    # for observation in seedObservations:
    #     line = json.dumps(observation) + "\n"
    #     finalObj.write(line + ",\n")

    wifiap.createWiFiObservations(dt, end, step, dataDir)
    wifiObj = open("data/wifiAPData.json")
    for line in wifiObj:
        finalObj.write(line + ",\n")
    wifiObj.close()

    wemo.createWemoObservations(dt, end, step, dataDir)
    wemoObj = open("data/wemoData.json")
    for line in wemoObj:
        finalObj.write(line + ",\n")
    wemoObj.close()

    temperature.createTemperatureObservations(dt, end, step, dataDir)
    temperatureObj = open("data/temperatureData.json")
    for line in temperatureObj:
        finalObj.write(line + ",\n")
    temperatureObj.close()

    line = json.loads(line)
    line['id'] = str(uuid.uuid4())

    finalObj.write(json.dumps(line))
    finalObj.write("\n]")

    finalObj.close()


def createIntelligentObservations(startTime, origDays, extendDays, origSpeed, extendSpeed,
                                  origWemo, extendWemo, origTemp, extendTemp, speedScaleNoise,
                               timeScaleNoise, deviceScale, dataDir, outputDir):

    wifiap.createIntelligentWiFiObs(startTime, extendDays, extendSpeed, outputDir)
    wemo.createIntelligentWemoObs(origDays, extendDays, origSpeed, extendSpeed,
                                  origWemo, extendWemo, speedScaleNoise,
                               timeScaleNoise, deviceScale, outputDir)
    temperature.createIntelligentTempObs(origDays, extendDays, origSpeed, extendSpeed,
                                   origTemp, extendTemp, speedScaleNoise,
                               timeScaleNoise, deviceScale, outputDir)
    line = None
    finalObj = open(outputDir + 'observation.json', 'w')
    finalObj.write("[\n")


    wifiObj = open("data/wifiAPData.json")
    for line in wifiObj:
        finalObj.write(line + ",\n")
    wifiObj.close()


    wemoObj = open("data/wemoData.json")
    for line in wemoObj:
        finalObj.write(line + ",\n")
    wemoObj.close()

    temperatureObj = open("data/temperatureData.json")
    for line in temperatureObj:
        finalObj.write(line + ",\n")
    temperatureObj.close()

    line = json.loads(line)
    line['id'] = str(uuid.uuid4())

    finalObj.write(json.dumps(line))
    finalObj.write("\n]")

    finalObj.close()
