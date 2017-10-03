import ConfigParser
import shutil
import users, sensors, observations
import datetime

configFile = "config.ini"
common = ["location.json", "infrastructureType.json", "infrastructure.json",
          "sensorType.json", "group.json", "platformType.json"]


def readConfiguration():
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


def createSensors(config):
    sensors.createSensors(int(config['sensors']['wifiap']), int(config['sensors']['wemo']),
                          int(config['sensors']['temperature']), config['others']["data-dir"],
                          config['others']["output-dir"])


def createObservations(config):
    start = datetime.datetime.strptime(config['observation']['start_timestamp'], "%Y-%m-%d %H:%M:%S")
    end = start + datetime.timedelta(days=int(config['observation']['days']))
    step =  datetime.timedelta(seconds=int(config['observation']['step']))
    observations.createObservations(start, end, step, config['others']["data-dir"], config['others']["output-dir"])

if __name__ == "__main__":
    configDict = readConfiguration()
    copyFiles(common, configDict['others']["data-dir"], configDict['others']["output-dir"])
    createUsers(configDict)
    createSensors(configDict)
    createObservations(configDict)