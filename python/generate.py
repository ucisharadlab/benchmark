import ConfigParser
import datetime
import shutil
import sys

from metadata import sensors, users
from semanticobservation import semanticobservations

common = ["location.json", "infrastructure.json", "mobileland.json",
          "wifiMap.json", "occupancy.json", "user.json"]


def readConfiguration(configFile):
    Config = ConfigParser.ConfigParser()
    Config.read(configFile)

    print ("Reading Configuration File")
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
    print ("Copying Source Files")

    for file in files:
        shutil.copy2(src+file, dest+file)


def createUsers(config):
    users.createUsers(int(config['others']['users']), config['others']["data-dir"], config['others']["output-dir"])


def createVisits(config):
    start = datetime.datetime.strptime(config['observation']['start_timestamp'], "%Y-%m-%d %H:%M:%S")
    end = start + datetime.timedelta(days=int(config['observation']['days']))
    users.createVisits(int(config['others']['users']), int(config['others']["visits"]), start, end,
                       config['others']["data-dir"], config['others']["output-dir"])


def createSensors(config, pattern):
    if pattern == "random":
        sensors.createSensors(int(config['sensors']['wifiap']), int(config['sensors']['wemo']),
                          int(config['sensors']['temperature']), config['others']["data-dir"],
                          config['others']["output-dir"])
    elif pattern == "intelligent":
        sensors.createIntelligentSensors(int(config['sensors']['wemo']),
                          int(config['sensors']['temperature']), config['others']["data-dir"],
                          config['others']["output-dir"])


def createSemanticObservations(config, pattern):
    start = datetime.datetime.strptime(config['observation']['start_timestamp'], "%Y-%m-%d %H:%M:%S")
    end = start + datetime.timedelta(days=int(config['observation']['days']))
    step = datetime.timedelta(seconds=int(config['observation']['step']))

    if pattern == "random":
        semanticobservations.createObservations(start, end, step, config['others']["data-dir"], config['others']["output-dir"])
    elif pattern == "intelligent":
        semanticobservations.createIntelligentObservations(
            start, end,
            int(config['seed']["days"]), int(config['observation']["days"]),
            int(config['seed']["step"]), int(config['observation']["step"]),
            float(config['seed']["speed-noise"]), float(config['seed']["time-noise"]),
            config['others']["data-dir"], config['others']["output-dir"])


def directoryClenaup(config):
    pass


if __name__ == "__main__":
    configFile = sys.argv[1]
    configDict = readConfiguration(configFile)
    pattern = configDict['others']["pattern"]
    copyFiles(common, configDict['others']["data-dir"], configDict['others']["output-dir"])

    createUsers(configDict)
    createVisits(configDict)
    # createSemanticObservations(configDict, pattern)
