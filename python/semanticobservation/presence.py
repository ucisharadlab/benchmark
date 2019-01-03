import datetime
import random
import json
import uuid
import numpy as np

from utils import helper
from utils.helper import toUTC


def createPresence(numUsers, avgUsersPerDay, dt, end, dataDir):

    with open(dataDir + 'mobileland.json') as data_file:
        rooms = json.load(data_file)

    users = list(range(1, numUsers+1))
    numRooms = len(rooms)
    numUsers = len(users)

    fpObj = open('data/presenceData.json', 'w')

    print ("Creating Random Presence Data " + str(numUsers))

    count = 0
    while dt < end:
        for j in np.random.choice(numUsers, avgUsersPerDay+np.random.randint(-50, 10), replace=False):
            startInternal, endInternal = randomTimeRange(dt)
            while startInternal < endInternal:
                step = datetime.timedelta(minutes=np.random.randint(15, 80))
                sobs = {
                    "id": count+1,
                    "start_timestamp": toUTC(startInternal)-7*3600,
                    "end_timestamp": toUTC(startInternal+step)-7*3600,
                    "user": users[j],
                    "location": rooms[random.randint(0, numRooms-1)]['name']
                }
                fpObj.write(json.dumps(sobs) + '\n')
                count += 1
                startInternal += step

        dt += datetime.timedelta(days=1)

    fpObj.close()


def randomTimeRange(start):
    t1 = start + datetime.timedelta(minutes=np.random.randint(8*60, 10*60))
    t2 = t1 + datetime.timedelta(minutes=np.random.randint(6*60, 10*60))
    return t1, t2
