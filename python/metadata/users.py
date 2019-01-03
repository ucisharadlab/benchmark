import random
import json
import uuid
import numpy as np
import datetime

from utils.helper import toUTC, jsonToCsv


def createUsers(numUsers, src, dest):

    with open(src+'user.json') as data_file:
        users = json.load(data_file)

    print ("Creating Users")

    numOldUsers = len(users)
    for i in range(numUsers-numOldUsers):
        id = str(uuid.uuid4())
        user = {
            "id": i + numOldUsers + 1,
            "googleAuthToken": id,
            "name": "simUser{}".format(i),
            "emailId": "simUser{}@spawar.org".format(i) ,
        }
        users.append(user)

    with open(dest + 'user.json', 'w') as writer:
        json.dump(users, writer, indent=4)
    jsonToCsv(dest + 'user.json', dest + 'user.csv')


def createVisits(numUsers, avgVisitsPerDay, startDate, endDate, src, dest):

    with open(src+'user.json') as data_file:
        users = json.load(data_file)

    print ("Creating Visits")

    #numUsers += len(users)

    hosts = map(lambda x: x+1, np.random.choice(numUsers, numUsers/4, replace=False))
    visitors = filter(lambda x: x not in hosts, range(1, numUsers+1))

    dt = startDate
    step = datetime.timedelta(days=1)
    visits = []

    i = 0
    while dt < endDate:
        numVisitsToday = avgVisitsPerDay + np.random.randint(-10, 10)
        hostsToday = np.random.choice(len(hosts), numVisitsToday, replace=True)
        visitorsToday = np.random.choice(len(visitors), numVisitsToday, replace=False)
        np.random.shuffle(hostsToday)
        np.random.shuffle(visitorsToday)

        for j in range(numVisitsToday):
            start, end = randomTimeRange(dt)
            visit = {
                "id": i+1,
                "host_id": hosts[hostsToday[j]],
                "visitor_id": visitors[visitorsToday[j]],
                "start_timestamp": toUTC(start),
                "end_timestamp": toUTC(end)
            }
            visits.append(visit)
            i += 1

        dt += step

    with open(dest + 'visits.json', 'w') as writer:
        json.dump(visits, writer, indent=4)
    jsonToCsv(dest + 'visits.json', dest + 'visits.csv')


def randomTimeRange(start):
    t1 = start + datetime.timedelta(minutes=np.random.randint(8*60, 12*60))
    t2 = t1 + datetime.timedelta(minutes=np.random.randint(2*60, 8*60))
    return t1, t2
