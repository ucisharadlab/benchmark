import json
import uuid
import numpy as np
import datetime as dt
from numpy.random import normal as N, uniform as U, exponential as Exp
from numpy.random import choice
from datetime import timedelta as td
from collections import defaultdict
from kerneltree import IntervalTree # need to `pip install kerneltree`
from itertools import ifilter

from utils.helper import loadJSON, dumpJSON, toUTC, toDatetime, toTime, dumpTXT
from utils.dijkstra import makeSpacesGraph, allPairsShortestPath, shortestPath

# For profiling: 
import time
import cProfile, pstats

# TODO: Aside
# 1. Optimization: Allow the user to choose an event first, and then decide
# if they can go to it; else choose a different event
# 2. Use times in UTC format
# 3. End time frame delta
# 4. Simulator: Space Generation

# TODO: Main
# 1. I need to get the AIRPORT scenario working
# 2. I need to record the amount of time people spend in their office

printTrajectories = False

# Right now, here are the optimizations that can take place: 
showProfiler = False

cacheAllPairsShortestPaths = False
randomSpaceTravelTime = False

# print('Options:')
# print('cacheAllPairsShortestPaths: ', cacheAllPairsShortestPaths)
# print('randomSpaceTravelTime', randomSpaceTravelTime)

class User:
    def __init__(self, user, entrySpaces):
        self.id = user['id']
        self.startTime = toTime(user['startTime'])
        self.endTime = toTime(user['endTime'])
        self.startTimeStddev = int(user['startTimeStddev'])
        self.endTimeStddev = int(user['endTimeStddev'])
        self.type = user['type']
        self.startSpace = entrySpaces[choice(len(entrySpaces))]
        self.currentSpace = self.startSpace
        self.periodicEvents = set()
        self.restroomTime= td(minutes=int(round(abs(N(120,5)))))
        self.onLeisure = False
        self.startLeisureTime = None
        self.currentEventid = None
        self.previousWifiAP = None
        self.preferredSpaces = user['preferredSpaces']

    def getNewDayStartEnd(self):
        startDelta = int(round(Exp(self.startTimeStddev))) * choice([-1, 1])
        startTime = self.startTime + td(minutes=startDelta)
        endDelta = int(round(Exp(self.endTimeStddev))) * choice([-1, 1])
        endTime = self.endTime + td(minutes=endDelta)
        return startTime.time(), endTime.time()

    def resetRestroomTime(self):
        self.restroomTime= td(minutes=int(round(abs(N(180,5)))))

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
              self.currentSpace, self.periodicEvents, str(self.restroomTime))


class Event:
    def __init__(self, event):
        self.id = event['id']
        self.type = event['type']
        self.spaces = event['space']
        self.isPeriodic = True if event['isPeriodic'] == 'True' else False
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
        self.type = space['type']
        self.capacity = space['capacity']
        self.sourceObservations = space['source_observations']

        for space in spaceOntology['neighbors']:
            if self.id == space['id']:
                self.neighbors = space['next']
                break
        else:
            self.neighbors = []
        self.occupancy = IntervalTree()

    def insertOccupancy(self, startTime, endTime):
        self.occupancy.add(toUTC(startTime), toUTC(endTime), 1)

    def getCurrentOccupancy(self, time):
        return len(self.occupancy.search(toUTC(time), toUTC(time)))

    def isFull(self, time):
        return self.capacity != -1 and \
                self.getCurrentOccupancy(time) >= self.capacity 

    def __str__(self):
        return \
'''Space:
  id = {}
  type = {}
  capacity = {}
  neighbors = {}'''.format(self.id, self.type, self.capacity, self.neighbors)



class TrajectoriesData:
    def __init__(self, startDtime, endDtime, src, dest):
        self.startDtime, self.endDtime = startDtime, endDtime
        self.src, self.dest = src, dest

        self.count = 0
        self.movements = []
        self.groundTruth = {'G': {}}
        self.observations = {'O': {}}

        self.userTypes = {utype['name']: utype \
                for utype in loadJSON(src+'UserTypes.json')}

        self.events = {event['id']: Event(event) \
                for event in loadJSON(src+'Events.json')}
        self.spaceOntology = loadJSON(src+'SpaceOntology.json')

        self.spaces = {space['id']: Space(space, self.spaceOntology) \
                for space in loadJSON(src+'Spaces.json')}

        self.entrySpaces = filter(lambda s: s < 0, self.spaces)
        self.users = {user['id']: User(user, self.entrySpaces) \
                for user in loadJSON(src+'Users.json')}
        self.eventTypes = { etype['name']: \
                {pr['type']: pr['prob'] for pr in etype['probabilities']} \
                for etype in loadJSON(src+'EventTypes.json')}

        self.spacesGraph = makeSpacesGraph(self.spaces.values())

        # Key: (startSpaceid, endSpaceid)
        # Value: (pathCost, path)
        self.shortestPaths = allPairsShortestPath(self.spacesGraph)

        # Cache these so that we are not constantly recreating them
        self.td1Second = td(seconds=1)
        self.td1Minute = td(minutes=1)
        self.td10Minutes = td(minutes=10)
        self.td1Day = td(days=1)
        self.firstMinute = dt.time(hour=0, minute=0)
        self.lastMinute = dt.time(hour=23, minute=59)

        # And this is for trajectory data
        self.currentDtime = None
        self.currentUser = None

        spaceJson = loadJSON(src+'Spaces.json')

        sourceObs = defaultdict(list)

        for sp in spaceJson:
            for so in sp['source_observations']:
                sourceObs[int(so)].append(sp['id'])

        dumpJSON(self.dest+'wifi_coverage.json', sourceObs)

        semanticsSpaces = {}
        for sp in spaceJson:
            semanticsSpaces[int(sp['id'])] = sp['type'] if sp['type'] else "null"

        dumpJSON(self.dest+'space_semantics.json', semanticsSpaces)

        userTypeDict = {}
        for u in self.users.values():
            userTypeDict[int(u.id)] = u.type

        dumpJSON(self.dest+'UserAndTypes.json', userTypeDict)

        self.userOccupancyFrequency = defaultdict(lambda : defaultdict(td))

    # TODO: store to database?
    def updateTrajectories(self, spaceid, startDtime, endDtime):
        self.userOccupancyFrequency[self.currentUser.id][spaceid] += \
                (endDtime - startDtime)
        self.currentUser.currentSpace = spaceid
        self.currentUser.restroomTime -= (endDtime - startDtime + self.td1Second)
        if self.currentUser.restroomTime < self.td1Minute:
            self.currentUser.restroomTime = self.td1Minute
        self.movements.append(
            {
                "type": "BO_Presence",
                "payload": [
                    { "location": spaceid },
                    { "user": self.currentUser.id },
                    { "confidence": 1.0 },
                    { "start_timestamp": str(startDtime) },
                    { "end_timestamp": str(endDtime) },
                    { "debugging_event": self.currentUser.currentEventid }
                ],
                "timestamp": str(startDtime),
                "source_observations": self.spaces[spaceid].sourceObservations
            }
        )

        if endDtime - startDtime < self.td10Minutes:
            self.count += 1
            remainingTime = (endDtime - startDtime).seconds / 60
            recordTime = startDtime + \
                    td(minutes=int(round(U(0, remainingTime))))
            if self.spaces[spaceid].sourceObservations:
                if self.currentUser.previousWifiAP not in \
                        self.spaces[spaceid].sourceObservations:
                    obs = choice(self.spaces[spaceid].sourceObservations)
                    self.currentUser.previousWifiAP = obs
                self.groundTruth["G"]["g" + str(self.count)] = {
                    "location": spaceid,
                    "wifiAP": self.currentUser.previousWifiAP,
                    "user": self.currentUser.id,
                    "time": str(recordTime)
                }
                self.observations["O"]["o" + str(self.count)] = {
                    "wifiAP": self.currentUser.previousWifiAP,
                    "user": self.currentUser.id,
                    "time": str(recordTime)
                }
                if printTrajectories:
                    pass
                    print('User {}, w/prof {} at space {} from {} to {}, observed by {}'.format(\
                        self.currentUser.id, self.currentUser.type, \
                        spaceid, startDtime, endDtime, self.currentUser.previousWifiAP))
        else:
            numObservations = 3
            recordTime = startDtime
            for i in range(numObservations):
                self.count += 1
                start = recordTime
                end = start + td(minutes=max(int(round(Exp(10))), 1))
                recordTime += td(minutes=max(int(round(Exp(10))), 1))
                if self.spaces[spaceid].sourceObservations:
                    if self.currentUser.previousWifiAP not in \
                            self.spaces[spaceid].sourceObservations:
                        obs = choice(self.spaces[spaceid].sourceObservations)
                        self.currentUser.previousWifiAP = obs
                    self.groundTruth["G"]["g" + str(self.count)] = {
                        "location": spaceid,
                        "wifiAP": self.currentUser.previousWifiAP,
                        "user": self.currentUser.id,
                        "time": str(recordTime)
                    }
                    self.observations["O"]["o" + str(self.count)] = {
                        "wifiAP": self.currentUser.previousWifiAP,
                        "user": self.currentUser.id,
                        "time": str(recordTime)
                    }
                if printTrajectories:
                    pass
                    print('User {}, w/prof {} at space {} from {} to {}, observed by {}'.format(\
                        self.currentUser.id, self.currentUser.type, \
                        spaceid, start, end, self.currentUser.previousWifiAP))

        if printTrajectories:
            print('User {}, w/prof {} at space {} from {} to {}, observed by {}'.format(\
                self.currentUser.id, self.currentUser.type, \
                spaceid, startDtime, endDtime, self.currentUser.previousWifiAP))

    def dump(self):
        dumpJSON(self.dest+'Trajectories.json', self.movements)
        dumpJSON(self.dest+'GroundTruth.json', self.groundTruth)
        dumpTXT(self.dest+'GroundTruth.txt', self.groundTruth)
        dumpJSON(self.dest+'Observations.json', self.observations)
        dumpTXT(self.dest+'Observations.txt', self.observations)

        duration = self.endDtime - self.startDtime
        totalSeconds = duration.seconds + 3600*24*duration.days

        convert = lambda x : round((x.seconds + 3600.0*24*x.days) / totalSeconds, 2)
        
        for freq in self.userOccupancyFrequency:
            self.userOccupancyFrequency[freq] = \
                {prefSpace: convert(self.userOccupancyFrequency[freq][prefSpace])
                    for prefSpace in sorted(\
                    ifilter(lambda y : y > 0, \
                    self.userOccupancyFrequency[freq]), \
                key=lambda x : -self.userOccupancyFrequency[freq][x])[:3]}

        dumpJSON(self.dest+'UsersByFrequency.txt', self.userOccupancyFrequency)

    def generateUserTrajectories(self):
        for i, userid in enumerate(self.users, 1):
            self.currentUser = self.users[userid]
            self.currentDtime = None
            self._generateUserTrajectory()

            if i % 20 == 0:
                print('on user', i)

    def _generateUserTrajectory(self):
        currentDate = self.startDtime.date()
        while currentDate < self.endDtime.date():
            self._generateUserTrajectoryForDay(currentDate)
            currentDate += self.td1Day

    def _generateUserTrajectoryForDay(self, currentDate):
        startTime, endTime = self.currentUser.getNewDayStartEnd()
        self.currentDtime = dt.datetime.combine(currentDate, startTime)
        self.updateTrajectories(self.currentUser.startSpace, \
                dt.datetime.combine(self.currentDtime.date(), \
                self.firstMinute), \
                self.currentDtime - self.td1Second)
        endDtime = dt.datetime.combine(currentDate, endTime)
        self.currentUser.resetRestroomTime()
        while self.currentDtime < endDtime:
            self._assignUserToEvents()
        self._moveUserTowardsSpace(self.currentUser.startSpace)
        self.updateTrajectories(self.currentUser.startSpace, \
                self.currentDtime, \
                dt.datetime.combine(self.currentDtime.date(), \
                self.lastMinute))

    def _assignUserToEvents(self):
        eventid = self._getAttendablePEvent() or self._getAttendableEvent()
        self.currentUser.currentEventid = eventid
        if self.events[eventid].type == 'leisure':
            self._startLeisure()
        else:
            self._stopLeisure()
            self._assignUserToEvent(eventid)
    
    def _getAttendablePEvent(self):
        return next(ifilter(lambda eid: \
            str(self.currentDtime.weekday()) in self.events[eid].periodDays and \
            self._userCanArriveToPEvent(eid) and \
            self._enoughSpaceForUser(eid),
            self.currentUser.periodicEvents), None)

    def _getAttendableEvent(self):
        eventids = filter(lambda eid: \
                self._userCanArriveToEvent(eid) and \
                self._userDoesNotHaveTimeConflicts(eid) and \
                self._enoughSpaceForUser(eid),\
                self.events)
        eventsByType = defaultdict(list)
        for eid in eventids:
            etype = self.events[eid].type
            eventsByType[etype].append( \
                    [eid, self.eventTypes[etype][self.currentUser.type]])
        prSum = 0
        for probabilities in eventsByType.values():
            for _, pr in probabilities:
                prSum += pr

        probabilities = []
        for probs in eventsByType.values():
            pairSum = 0
            for pair in probs:
                pair[1] /= prSum
                pairSum += pair[1]
            probabilities.append(pairSum)

        names = list(eventsByType)
        group = choice(len(names), p=probabilities)

        index = choice(len(eventsByType[names[group]]))
        eventid = eventsByType[names[group]][index][0]
        return eventid
                
    def _userCanArriveToPEvent(self, eventid):
        travelTime = self._getTravelTime(eventid)
        if travelTime is None:
            return False
        startTime = (self.events[eventid].startTime - self.td10Minutes).time()
        endTime = (self.events[eventid].endTime - self.td10Minutes).time()
        arrivalTime = (self.currentDtime + travelTime).time()
        return startTime <= arrivalTime <= endTime

    def _userCanArriveToEvent(self, eventid):
        if self.events[eventid].isPeriodic:
            return self._userCanArriveToPEvent(eventid)
        travelTime = self._getTravelTime(eventid)
        if travelTime is None:
            return False
        startTime = self.events[eventid].startTime - self.td10Minutes
        endTime = self.events[eventid].endTime - self.td10Minutes
        arrivalTime = self.currentDtime + travelTime
        return startTime <= arrivalTime <= endTime

    def _getTravelTime(self, eventid):
        index = choice(len(self.events[eventid].spaces))
        sid = self.events[eventid].spaces[index]
        hops = self.shortestPaths[(self.currentUser.currentSpace, sid)][0]
        if hops is None:
            return None
        return td(minutes=hops)

    def _userDoesNotHaveTimeConflicts(self, eventid):
        weekday = str(self.currentDtime.weekday())
        start1 = self.events[eventid].startTime.time()
        end1 = self.events[eventid].endTime.time()
        for eid in self.currentUser.periodicEvents:
            start2 = self.events[eid].startTime.time()
            end2 = self.events[eid].endTime.time()
            if weekday in self.events[eventid].periodDays and \
                    max(start1, start2) < min(end1, end2):
                return False
        return True

    def _enoughSpaceForUser(self, eventid):
        occ = self.events[eventid].occupancy
        cap = self.events[eventid].capacity
        type = self.currentUser.type
        userTypeCapacity = occ[type] < cap[type]['hi'] or cap[type]['hi'] == -1
        if not userTypeCapacity:
            return False
        spaces = next(ifilter(lambda sid: \
                not self.spaces[sid].isFull(self.currentDtime), \
                self.events[eventid].spaces), False)
        return spaces 
        spaces = filter(lambda sid: \
                    not self.spaces[sid].isFull(self.currentDtime), \
                    self.events[eventid].spaces)
        return spaces 


    def _assignUserToEvent(self, eventid):
        if self.events[eventid].isPeriodic:
            self.currentUser.periodicEvents.add(eventid)
        endDtime, spaceid = self._getEventDetailsForUser(eventid)
        self._moveUserTowardsSpace(spaceid)
        self._userAttendsEvent(endDtime, spaceid)

    def _getEventDetailsForUser(self, eventid):
        # TODO: change delta? 
        delta = lambda : int(round(Exp(3))) * choice([-1, 1])
        endTime = self.events[eventid].endTime + td(minutes=delta())
        endTime = max((self.currentDtime + self.td1Second).time(), 
                    endTime.time())
        # TODO: there is some problem where the above check for whether the 
        # user can attend the event (which also checks if there is enough
        # space) for the user is failing. TBD... but for now we just set the
        # spaces that the user can choose from to be any of the spaces the 
        # event can take place in
        spaces = filter(lambda sid: \
                not self.spaces[sid].isFull(self.currentDtime), \
                self.events[eventid].spaces)

        space = self._getSpaceDetails(spaces) # spaces[choice(len(spaces))]
        if self.events[eventid].isPeriodic:
            endTime = dt.datetime.combine( \
                    self.currentDtime.date(), endTime)
        return endTime, space

    def _getSpaceDetails(self, spaces):
        overlap = filter(lambda s: \
                s in self.currentUser.preferredSpaces, spaces)
        if overlap:
            spaces = overlap
        return spaces[choice(len(spaces))]

    def _moveUserTowardsSpace(self, spaceid):
        path = self.shortestPaths[(self.currentUser.currentSpace, spaceid)][1]
        for spaceid in path[1:-1]:
            # TODO: change amount of time it takes to go between adjacent rooms
            self.updateTrajectories(spaceid, \
                    self.currentDtime, \
                    self.currentDtime + self.td1Minute - self.td1Second)
            self.spaces[spaceid].insertOccupancy(
                    self.currentDtime, 
                    self.currentDtime + self.td1Minute - self.td1Second)
            self.currentDtime += self.td1Minute

    def _userAttendsEvent(self, endTime, spaceid):
        if self.currentUser.restroomTime < \
                (endTime - self.currentDtime - self.td10Minutes):
            restroomTime = self.currentDtime + self.currentUser.restroomTime
            self._userAttendsFirstHalfOfEvent(spaceid, restroomTime)
            self._userGoesToRestroom(spaceid)
            self._userAttendsSecondHalfOfEvent(spaceid, endTime)
        else:
            self.spaces[spaceid].insertOccupancy(self.currentDtime, endTime)
            self.updateTrajectories(spaceid, \
                    self.currentDtime, endTime - self.td1Second)
            self.currentDtime = endTime

    def _userAttendsFirstHalfOfEvent(self, spaceid, restroomTime):
        self.spaces[spaceid].insertOccupancy(self.currentDtime, restroomTime)
        self.updateTrajectories(spaceid, \
                self.currentDtime, restroomTime - self.td1Second)
        self.currentDtime = restroomTime

    def _userAttendsSecondHalfOfEvent(self, spaceid, endTime):
        self.spaces[spaceid].insertOccupancy(self.currentDtime, endTime)
        self.updateTrajectories(spaceid, \
                self.currentDtime, endTime - self.td1Second)
        self.currentDtime = endTime

    def _userGoesToRestroom(self, spaceid):
        nearestRestroom = self._getNearestRestroom(spaceid)
        self._moveUserTowardsSpace(nearestRestroom)
        restroomEnd = self.currentDtime + td(minutes=abs(max(int(round(Exp(5))), 3)))
        self.spaces[spaceid].insertOccupancy( \
                self.currentDtime, restroomEnd)
        self.updateTrajectories(nearestRestroom, \
                self.currentDtime, restroomEnd - self.td1Second)
        self.currentDtime = restroomEnd
        self._moveUserTowardsSpace(spaceid)
        self.currentUser.resetRestroomTime()
    
    # TODO: get min distance restroom? 
    def _getNearestRestroom(self, spaceid):
        restrooms = filter(lambda s: self.spaces[s].type == 'restroom', \
                self.spaces)
        return restrooms[np.random.choice(len(restrooms))]

    def _startLeisure(self):
        if not self.currentUser.onLeisure:
            self.currentUser.onLeisure = True
            self.currentUser.leisureSpace = choice(self.entryExitSpace)
            self._moveUserTowardsSpace(self.currentUser.leisureSpace)
            self.currentUser.startLeisureTime = self.currentDtime
        self.currentDtime += td(minutes=int(round(N(15, 2))))

    def _stopLeisure(self):
        if self.currentUser.onLeisure:
            self.updateTrajectories(self.currentUser.leisureSpace, \
                    self.currentUser.startLeisureTime, \
                    self.currentDtime - self.td1Second)
            self.currentUser.onLeisure = False
            self.currentUser.startLeisureTime = None

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


