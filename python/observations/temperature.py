import datetime
import random
import json
import uuid
import numpy as np
from utils import helper
from extrapolate import Scale


def createTemperatureObservations(dt, end, step, dataDir):

    with open(dataDir + 'sensor.json') as data_file:
        data = json.load(data_file)

    sensors = []

    for sensor in data:
        if sensor['type_']['id'] == "Thermometer":
            sensors.append(sensor)
    num = len(sensors)

    fpObj = open('data/temperatureData.json', 'w')

    print ("Creating Random Temperature Observations")

    count = 0
    while dt < end:

        for i in np.random.choice(num, num/8, replace=False):
            pickedSensor = helper.deleteSensorAttributes(sensors[i])
            id = str(uuid.uuid4())
            obs = {
                "id": id,
                "timeStamp": dt.strftime('%Y-%m-%d %H:%M:%S'),
                "sensor": pickedSensor,
                "payload": {
                    "temperature": random.randint(1, 100)
                }
            }
            fpObj.write(json.dumps(obs) + '\n')

            if count % 200000 == 0:
                print ("{} Random Temperature Observations".format(count))
            count += 1

        dt += step

    fpObj.close()


def createIntelligentTempObs(origDays, extendDays, origSpeed, extendSpeed, origSensor, extendSensor, speedScaleNoise,
                               timeScaleNoise, deviceScaleNoise, dataDir):

    with open(dataDir + 'observation.json') as data_file:
        observations = json.load(data_file)

    seedFile = open("data/seedTemperature.json", "w")
    for observation in observations:
        if observation['sensor']['type_']['id'] == "Thermometer":
            seedFile.write(json.dumps(observation) + '\n')
    seedFile.close()

    seedFile = "data/seedTemperature.json"
    outputFile = "data/temperatureData.json"
    scale = Scale(dataDir, seedFile, outputFile, origDays, extendDays, origSpeed, extendSpeed,
                  origSensor, extendSensor, "temperature", speedScaleNoise, timeScaleNoise,
                  deviceScaleNoise, int)

    scale.speedScale()
    scale.deviceScale()
    scale.timeScale()
