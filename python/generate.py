import ConfigParser
import datetime
import shutil
import sys

from metadata import sensors, users
from observations import observations
from semanticobservation import semanticobservations
from queries import Queries

common = ["location.json", "infrastructureType.json", "infrastructure.json",
          "sensorType.json", "group.json", "platformType.json", "virtualSensorType.json", "virtualSensor.json",
          "semanticObservationType.json", "wifiMap.json", "observation.json", "semanticObservation.json"]


def readConfiguration(configFile):
    Config = ConfigParser.ConfigParser()
    Config.read(configFile)

    configDict = {section:{} for section in Config.sections()}

    for section in Config.sections():
        options = Config.options(section)
        for option in options:
            try:
                configDict[section][option] = Config.get(section, option)
            except Exception as e:
                configDict[section][option] = None

    return configDict


def copyFiles(files, src, dest):
    for file in files:
        shutil.copy2(src+file, dest+file)


def createUsers(config):
    users.createUsers(int(config['others']['users']), config['others']["data-dir"], config['others']["output-dir"])


def createSensors(config, pattern):
    if pattern == "random":
        sensors.createSensors(int(config['sensors']['wifiap']), int(config['sensors']['wemo']),
                          int(config['sensors']['temperature']), config['others']["data-dir"],
                          config['others']["output-dir"])
    elif pattern == "intelligent":
        sensors.createIntelligentSensors(int(config['sensors']['wemo']),
                          int(config['sensors']['temperature']), config['others']["data-dir"],
                          config['others']["output-dir"])


def createObservations(config, pattern):
    start = datetime.datetime.strptime(config['observation']['start_timestamp'], "%Y-%m-%d %H:%M:%S")
    end = start + datetime.timedelta(days=int(config['observation']['days']))
    step = datetime.timedelta(seconds=int(config['observation']['step']))

    if pattern == "random":
        observations.createObservations(start, end, step, config['others']["data-dir"], config['others']["output-dir"])
    elif pattern == "intelligent":
        observations.createIntelligentObservations(start,
                                                   int(config['seed']["days"]), int(config['observation']["days"]),
                                                   int(config['seed']["step"]), int(config['observation']["step"]),
                                                   int(config['seed']["wemo"]), int(config['sensors']["wemo"]),
                                                   int(config['seed']["temperature"]), int(config['sensors']["temperature"]),
                                                   float(config['seed']["speed-noise"]), float(config['seed']["time-noise"]),
                                                   float(config['seed']["sensor-noise"]), config['others']["data-dir"],
                                                   config['others']["output-dir"])


def createSemanticObservations(config, pattern):
    start = datetime.datetime.strptime(config['observation']['start_timestamp'], "%Y-%m-%d %H:%M:%S")
    end = start + datetime.timedelta(days=int(config['observation']['days']))
    step = datetime.timedelta(seconds=int(config['observation']['step']))

    if pattern == "random":
        semanticobservations.createObservations(start, end, step, config['others']["data-dir"], config['others']["output-dir"])
    elif pattern == "intelligent":
        semanticobservations.createIntelligentObservations(int(config['seed']["days"]), int(config['observation']["days"]),
                                                           int(config['seed']["step"]), int(config['observation']["step"]),
                                                           float(config['seed']["speed-noise"]), float(config['seed']["time-noise"]),
                                                           config['others']["data-dir"], config['others']["output-dir"])


def createQueries(config):
    q = Queries(int(config['query']['runs']), config['others']['output-dir'], config['query']['output-dir'],
                config['observation']['start_timestamp'], int(config['observation']['days']),
                int(config['query']['num-locations']), int(config['query']['num-sensors']),
                int(config['query']['time-delta']))
    q.generateQueries()

if __name__ == "__main__":
    configFile = sys.argv[1]
    configDict = readConfiguration(configFile)
    pattern = configDict['others']["pattern"]
    # copyFiles(common, configDict['others']["data-dir"], configDict['others']["output-dir"])
    #
    # createUsers(configDict)
    #
    # createSensors(configDict, pattern)
    #
    # createObservations(configDict, pattern)
    # createSemanticObservations(configDict, pattern)
    createQueries(configDict)