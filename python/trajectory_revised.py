import json 
import uuid
import numpy as np
import datetime as dt
from numpy.random import exponential as Exp, normal as N
from numpy.random import choice
from datetime import timedelta as td
from collections import defaultdict

# C-based implementation of Interval Trees
from kerneltree import IntervalTree

from utils.helper import loadJSON, dumpJSON
from utils.helper import toUTC, toDatetime, toTime
from utils.dijkstra import makeSpacesGraph, allPairsShortestPath, shortestPath

# For profiling:
import time
import cProfile, pstats

# To show profiling
showProfiler = False

# Optimizations to take place
cacheAllPairShortestPaths = False

class User:
    def __init__(self, user, entrySpaces):
        self.id = user['id']
        self.startTime = toTime(user['startTime'])
        self.endTime = toTime(user['endTime'])
        self.startTimeStddev = int(user['startTimeStddev'])
        self.endTimeStddev = int(user['endTimeStddev'])
        self.type = user['type']
        self.currentSpace = entrySpaces[choice(len(entrySpaces))]
        self.periodicEvents = set()
        self.restroomLimit = td(minutes=int(round(N(240,30))))

    def getNewDayStartEnd(self):
        startDelta = int(round(Exp(self.startTimeStddev))) * choice([-1, 1])
        startTime = self.startTime + td(minutes=startDelta)

        endDelta = int(round(Exp(self.endTimeStddev))) * choice([-1, 1])
        endTime = self.endTime + td(minutes=endDelta)

        return startTime.time(), endTime.time()

    def resetRestroomLimit(self):
        self.restroomLimit = td(minutes=int(round(N(240,30))))

    def __str__(self):
        return \
'''User:
  id = {}
  startTime, endTime = {} -> {}
  startTimeSD, endTimeSD = {}, {}
  type = {}
  currentSpace = {}
  periodicEvents = {}
  restroomLimit = {}'''.format(self.id, \
            str(self.startTime), str(self.endTime), \
            self.startTimeStddev, self.endTimeStddev, self.type, \
            self.currentSpace, self.periodicEvents, str(self.restroomLimit))

class Event:
    def __init__(self, event):
        self.id = event['id']
        self.type = event['type']
        self.spaces = event['space']
        self.isPeriodic = 1 if event['isPeriodic'] == 'True' else 0
        if self.isPeriodic:
            self.periodDays = event['periodDays']
            self.startTime = toTime(event['startTime'])
            self.endTime = toTime(event['endTime'])
        else:
            self.periodDays = ''
            self.startTime = toDatetime(event['startTime'])
            self.endTime = toDatetime(event['endTime'])

        self.capacity = {cap['type']: {'lo': cap['lo'], 'hi': cap['hi']} \
                for cap in event['capacity']}
        self.occupancy = {cap['type']: 0 for cap in event['capacity']}

    def __str__(self):
        return \
'''Event:
  id = {}
  type = {}
  spaces = {}
  startTime, endTime = {} -> {}
  isPeriodic = {}
  periodDays = {}
  occupancy = {}'''.format(self.id, self.type, self.spaces, \
            self.startTime, self.endTime, self.isPeriodic, self.periodDays,\
            {'{}:{}/{}'.format( \
                    user, self.occupancy[user], self.capacity[user]['hi']) \
                    for user in self.occupancy})


class Space:
    def __init__(self, space, spaceOntology):
        self.id = space['id']
        self.type=  space['type']
        self.capacity = space['capacity']
        self.occupancy = IntervalTree()

        for space in spaceOntology['neighbors']:
            if self.id == space['id']:
                self.neighbors = space['next']
                break
        else:
            self.neighbors = []

    def insertOccupancy(self, startDtime, endDtime):
        self.occupancy.add(toUTC(startDtime), toUTC(endDtime), 1)

    def getCurrentOccupancy(self, dtime):
        return len(self.occupancy.search(toUTC(dtime), toUTC(dtime)))

    def isFull(self, dtime):
        return self.capacity != -1 and \
                self.getCurrentOccupancy(dtime) >= self.capacity

    def __str__(self):
        return \
'''Space:
  id = {}
  type = {}
  capacity = {}
  neighbors = {}'''.format(self.id, self.type, self.capacity, self.neighbors)


class TrajectoriesData:
    def __init__(self, startDtime, endDtime, src, dest):
        self.startDtime = startDtime
        self.endDtime = endDtime
        self.src, self.dest = src, dest

        self.movements = []

        self.userTypes = {utype['name']: utype \
                for utype in loadJSON(src+'UserTypes.json')}

        self.events = {event['id']: Event(event) \
                for event in loadJSON(src+'Events.json')}
        self.spaceOntology = loadJSON(src+'SpaceOntology.json')

        self.spaces = {space['id']: Space(space, self.spaceOntology) \
                for space in loadJSON(src+'Spaces.json')}

        entrySpaces = filter(lambda s: s < 0, self.spaces)
        self.users = {user['id']: User(user, entrySpaces) \
                for user in loadJSON(src+'Users.json')}
        self.eventTypes = {etype['name']: \
                {pr['type']: pr['prob'] for pr in etype['probabilities']} \
                for etype in loadJSON(src+'EventTypes.json')}

        self.spacesGraph = makeSpacesGraph(self.spaces.values())

        self.shortestPaths = allPairsShortestPath(self.spacesGraph)

        # Cache these so that we are not constantly recreating them
        self.td1Second = td(seconds=1)
        self.td1Minute = td(minutes=1)
        self.td10Minutes = td(minutes=10)
        self.td1Day = td(days=1)

        # And this is for the trajectory data
        self.currentDtime = None
        self.currentUserid = None

    # TODO: Store to database
    def updateTrajectories(self, userid, spaceid, startTime, endTime):
        self.movements.append(
            {
                "type": "BO_Presence",
                "payload": [
                    { "location": spaceid },
                    { "user": userid },
                    { "confidence": 1.0 },
                    { "start_timestamp": str(startTime) },
                    { "end_timestamp": str(endTime) }
                ],
                "timestamp": str(startTime),
                "source_observations": []
            }
        )

    def dump(self):
        dumpJSON(self.dest+'Trajectories.json', self.movements)

    def generateUserTrajectories(self):
        for i, userid in enumerate(self.users, 1):
            self.currentDtime = None
            self.currentUser = self.users[userid]
            self._generateUserTrajectory()
            if i % 100 == 0:
                print('on user #', i)

    def _generateUserTrajectory(self):
        currentDate = self.startDtime.date()
        while currentDate < self.endDtime.date():
            self._generateUserTrajectoryForDay(currentDate)
            currentDate += self.td1Day

    def _generateUserTrajectoryForDay(self, currentDate):
        startTime, endTime = self.currentUser.getNewDayStartEnd()
        self.currentDtime = dt.datetime.combine(currentDate, startTime)
        endDtime = dt.datetime.combine(currentDate, endTime)
        while self.currentDtime < endDtime:
            self._assignUserToEvents()

    def _assignUserToEvents(self):
        pEvent = self._getUpcomingPEvent()
        if pEvent:
            self._recordAnyLeisureTime()
            self._attendPEvent(pEvent)
            return

        events = self._getUpcomingEvents()
        if events:
            event = self._getEventToAttend(events)
            self._recordAnyLeisureTime()
            self._attendEvent(event)
            return

        self._startLeisure()

    def _getUpcomingPEvent(self):
        return filter(lambda eid: self._userCanArriveToPEvent(eid),
                self.currentUser.periodicEvents)

    def _getUpcomingEvents(self):
        return filter(lambda eid: \
                self._userCanArriveToEvent(eid) and \
                self._enoughSpaceForUser(eid),
                self.events)
            

    def _attendPEvent(self):
        pass

    def _attendEvent(self):
        pass

    def _startLeisure(self):
        pass

    def _recordAnyLeisureTime(self):
        pass


def createTrajectories(startDtime, endDtime, src, dest):
    print('Creating Trajectories')

    if showProfiler:
        pr = cProfile.Profile()
        pr.enable()

    mData = TrajectoriesData(startDtime, endDtime, src, dest)

    print('... Generating User Trajectories')
    mData.generateUserTrajectories()

    if showProfiler:
        pr.disable()
        ps = pstats.Stats(pr).sort_stats('tottime').print_stats()

    print('... Saving Trajectories')
    mData.dump()
