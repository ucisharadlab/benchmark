import datetime
import random
import json
import wifiap, wemo, temperature


def createObservations(dt, end, step, dataDir, outputDir, types):
    line = None
    finalObj = open(outputDir + 'observation.json', 'w')
    finalObj.write("[\n")

    if 'wifiap' in types:
        wifiap.createWiFiObservations(dt, end, step, dataDir)
        wifiObj = open("data/wifiAPdata.json")
        for line in wifiObj:
            finalObj.write(line + ",\n")
        wifiObj.close()

    if 'wemo' in types:
        wemo.createWemoObservations(dt, end, step, dataDir)
        wemoObj = open("data/wemoData.json")
        for line in wemoObj:
            finalObj.write(line + ",\n")
        wemoObj.close()

    if 'temperature' in types:
        temperature.createTemperaturebservations(dt, end, step, dataDir)
        temperatureObj = open("data/temperatureData.json")
        for line in temperatureObj:
            finalObj.write(line + ",\n")
        temperatureObj.close()

    finalObj.write(line)
    finalObj.write("\n]")

    finalObj.close()
