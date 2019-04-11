import ConfigParser
import datetime
import shutil
import sys

from metadata import users, spaces, events
import trajectory
from utils.helper import toDatetime

specifications = ['UserTypes.json', 'SpaceTypes.json', 'EventTypes.json']
inputs = ['Users.json', 'Spaces.json', 'Events.json', 'SpaceOntology.json']
outputs = ['Movements.json']


def readConfiguration(configFile):
    print('Reading Configuration File')

    Config = ConfigParser.ConfigParser()
    Config.read(configFile)
    configDict = {section:{} for section in Config.sections()}

    for section in Config.sections():
        options = Config.options(section)
        for option in options:
            try:
                configDict[section][option] = Config.get(section, option)
            except:
                configDict[section][option] = None

    return configDict


def copyFiles(files, src, dest):
    print('Copying Source Files')

    for file in files:
        try:
            shutil.copy2(src+file, dest+file)
        except:
            print('... error copying file {}'.format(src+file))


def createUsers(config):
    start = toDatetime(config['observation']['start-timestamp'])
    end = start + datetime.timedelta(days=int(config['observation']['days']))
    users.createUsers(
            int(config['counts']['users']), start, end,
            config['others']['specification-dir'], 
            config['others']['input-dir'])

def createSpaces(config):
    pass


def createEvents(config):
    start = toDatetime(config['observation']['start-timestamp'])
    end = start + datetime.timedelta(days=int(config['observation']['days']))
    events.createEvents(
            int(config['counts']['events']), start, end,
            config['others']['specification-dir'], 
            config['others']['input-dir'])


def createTrajectoryData(config):
    start = toDatetime(config['observation']['start-timestamp'])
    end = start + datetime.timedelta(days=int(config['observation']['days']))
    trajectory.createTrajectories(
            start, end,
            config['others']['input-dir'], 
            config['others']['output-dir']);


if __name__ == '__main__':

    configDict = readConfiguration(sys.argv[1])
    # copyFiles(specifications, configDict['others']['specification-dir'], 
    #                 configDict['others']['input-dir'])

    createUsers(configDict)
    # createSpaces(configDict) # Not yet implemented
    # createEvents(configDict) 
    createTrajectoryData(configDict)
