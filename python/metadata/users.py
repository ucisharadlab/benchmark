import random
import json
import numpy as np
import datetime

from utils.helper import loadJSON, dumpJSON, toDatetime, toUTC

def updateUsers(users, userid, type, numEvents, expStartTime, expEndTime):
    users.append(
        {
            "id": userid,
            "type": type['name'],
            "numEvents": numEvents, 
            "startTime": str(expStartTime), 
            "endTime": str(expEndTime)
        }
    )

def _selectUserType(userTypes):
    distr = [float(utype['distribution']) for utype in userTypes]
    index = np.random.choice(len(userTypes), p=distr)
    return userTypes[index]

def _selectNumEvents(type, duration):
    avg, stddev = type['avg'], type['stddev']
    numEvents = abs(np.random.normal(avg, stddev))
    numEventsTimedelta = datetime.timedelta(
            hours=int(type['timedelta']['hours']), 
            minutes=int(type['timedelta']['minutes'])).seconds
    return int(round(duration.seconds * numEvents / numEventsTimedelta))

def _selectTimes(type):
    startStddev = int(type['expStartStddev']) # TODO: in minutes for now; change later?
    startTimeDelta = int(round(np.random.exponential(startStddev))) * np.random.choice([-1, 1])
    startTime = toDatetime(type['expStartTime']) + datetime.timedelta(minutes=startTimeDelta)

    endStddev = int(type['expEndStddev']) # TODO: in minutes for now; change later?
    endTimeDelta = int(round(np.random.exponential(endStddev))) * np.random.choice([-1, 1])
    endTime = toDatetime(type['expEndTime']) + datetime.timedelta(minutes=endTimeDelta)

    return startTime, endTime


def createUsers(numUsers, startTime, endTime, src, dest):
    print('Creating Users')

    userTypes = loadJSON(src+'UserTypes.json')

    users = []
    for i in range(numUsers):
        type = _selectUserType(userTypes)
        numEvents = _selectNumEvents(type, endTime - startTime)
        expStartTime, expEndTime = _selectTimes(type)
        updateUsers(users, i+1, type, numEvents, expStartTime, expEndTime)
    
    dumpJSON(dest + 'Users.json', users)

