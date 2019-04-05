import random
import json
import uuid
import numpy as np
import datetime
from collections import defaultdict

# Right now, here are the optimizations that can take place: 
showProfiler = True

useKernelTree = True
useDeterministicTimedeltaWhetherUserCanArriveToEvent = True

cacheAllPairsShortestPaths = False
randomSpaceTravelTime = False

print('Options:')
print('useKernelTree: ', useKernelTree)
print('cacheAllPairsShortestPaths: ', cacheAllPairsShortestPaths)
print('useDeterministicTimedeltaWhetherUserCanArriveToEvent', 
    useDeterministicTimedeltaWhetherUserCanArriveToEvent)
print('randomSpaceTravelTime', randomSpaceTravelTime)

if useKernelTree:
    from kerneltree import IntervalTree # C-based
else:
    from intervaltree import IntervalTree # Python Based


from utils.helper import loadJSON, dumpJSON, toUTC, toDatetime
from utils.dijkstra import makeSpacesGraph, allPairsShortestPath, shortestPath

# For profiling: 
import time
import cProfile, pstats

class User:
    def __init__(self, user, entryExitSpaces):
        self.id = user['id']
        self.startTime = toDatetime(user['startTime'])
        self.endTime = toDatetime(user['endTime'])
        self.numEvents = user['numEvents']
        self.type = user['type']

        self.currentSpace = entryExitSpaces[np.random.choice(len(entryExitSpaces))]


class Event:
    def __init__(self, event):
        self.id = event['id']
        self.type = event['type']
        self.spaces = event['space']
        self.startTime = toDatetime(event['startTime'])
        self.endTime = toDatetime(event['endTime'])
            
        self.capacity = {cap['type']: {'lo': cap['lo'], 'hi': cap['hi']} \
                for cap in event['capacity']}
        self.occupancy = {cap['type']: 0 for cap in event['capacity']}


class Space:
    def __init__(self, space, spaceOntology):
        self.id = space['id']
        self.type = space['type']
        self.capacity = space['capacity']

        for space in spaceOntology['neighbors']:
            if self.id == space['id']:
                self.neighbors = space['next']
                break
        else:
            self.neighbors = []
        self.occupancy= IntervalTree()

    def insertOccupancy(self, startTime, endTime):
        if useKernelTree:
            self.occupancy.add(toUTC(startTime), toUTC(endTime), 1)
        else:
            self.occupancy[str(startTime):str(endTime)] = 1

    def getCurrentOccupancy(self, time):
        if useKernelTree:
            return self.occupancy.search(toUTC(time), toUTC(time))
        else:
            return len(self.occupancy[str(time)])

    def isFull(self, time):
        return self.capacity != -1 and self.getCurrentOccupancy(time) >= self.capacity 


class TrajectoriesData:
    def __init__(self, startTime, endTime, src, dest):
        self.startTime = startTime
        self.endTime = endTime
        self.src = src
        self.dest = dest

        self.movements = []

        self.events = {event['id']: Event(event) for event in loadJSON(src+'Events.json')}
        self.spaceOntology = loadJSON(src+'SpaceOntology.json')

        self.spaces = {space['id']: Space(space, self.spaceOntology) \
                for space in loadJSON(src+'Spaces.json')}

        entryExitSpaces = filter(lambda s: s < 0, self.spaces)
        self.users = {user['id']: User(user, entryExitSpaces) \
                for user in loadJSON(src+'Users.json')}
        self.userTypes = loadJSON(src+'UserTypes.json')

        # TODO: optimization: calaculate a matrix with all pairs information
        # However, depending on the size of the input building, this may be too much
        self.spacesGraph = makeSpacesGraph(self.spaces.values())

        # We will select one of the following: 
        #   - all pairs shortest paths
        #   - shortest paths are cached / computed on the fly
        # The dictionary takes keys of tuple (startSpaceid, endSpaceid)
        # and returns tuples of (pathCost, 
        if cacheAllPairsShortestPaths:
            self.shortestPaths = {}
        else:
            self.shortestPaths = allPairsShortestPath(self.spacesGraph)

        self.timedelta10Minutes = datetime.timedelta(minutes=10)

    # TODO: store to database
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
            self._generateUserTrajectory(userid)

            if i % 100 == 0:
                print('on user #' + str(i))

    def _generateUserTrajectory(self, userid):
        currentTime = self.users[userid].startTime
        while currentTime < self.users[userid].endTime:
            leaveTime = self._assignUserToEvent(userid, currentTime)
            currentTime = leaveTime

    def _assignUserToEvent(self, userid, time):
        eventid = self._getEventToAttend(userid, time)
        endTime, spaceid = self._getEventDetailsForUser(userid, eventid, time)
        time = self._moveUserTowardsSpace(userid, spaceid, time)
        self._userAttendsEvent(userid, spaceid, time, endTime)
        return endTime

    def _getEventToAttend(self, userid, time):
        eventList = self._getAttendableEvents(userid, time)
        # TODO: change from random
        index = np.random.choice(len(eventList))
        return eventList[index]

    def _getAttendableEvents(self, userid, time):
        eventids = filter(lambda eid: \
                self._userCanArriveToEvent(userid, eid, time) and \
                self._enoughSpaceForUser(userid, eid, time), 
                self.events)
        return eventids

    def _userCanArriveToEvent(self, userid, eventid, time):
        # TODO: maybe let's have this be deterministic so that the time we take 
        # to get the simulation is lower
        if useDeterministicTimedeltaWhetherUserCanArriveToEvent:
            delta = self.timedelta10Minutes
        else:
            delta = datetime.timedelta(minutes=int(round(np.random.exponential(1))))
        # TODO: travel time to which one of the spaces for the event, if there are multiple? 

       # travelTime = min(self.shortestPaths[(self.users[userid].currentSpace, sid)][0] \
                # for sid in self.events[eventid].spaces)
        
        travelTime = self._getTravelTime(userid, eventid) # See note below

        startTimeframe = self.events[eventid].startTime - delta
        endTimeframe=  self.events[eventid].endTime
        return startTimeframe <= time + datetime.timedelta(minutes=travelTime) <= endTimeframe
        # return self._userCanArriveToEvent_Return(time, travelTime, startTimeframe, endTimeframe)

    def _userCanArriveToEvent_Return(self, time, travelTime, startTimeframe, endTimeframe):
        arrivalTime = time + datetime.timedelta(minutes=travelTime)
        if startTimeframe > arrivalTime:
            return False
        if endTimeframe < arrivalTime:
            return False
        return True
        # return startTimeframe <= time + datetime.timedelta(minutes=travelTime) <= endTimeframe
        

    # Note: this is really just so the profiler recognizes this as a separate function
    def _getTravelTime(self, userid, eventid):
        pathTuple = lambda path: (0 if path is None else len(path), path)
        if randomSpaceTravelTime:
            index = np.random.choice(len(self.events[eventid].spaces))
            sid = self.events[eventid].spaces[index]
            if cacheAllPairsShortestPaths:
                path = shortestPath(self.spacesGraph, self.users[userid].currentSpace, sid)
                toReturn = self.shortestPaths.setdefault(
                        (self.users[userid].currentSpace, sid), pathTuple(path))
                return len(toReturn)
            else:
                return self.shortestPaths[(self.users[userid].currentSpace, sid)][0]

        else:
            if cacheAllPairsShortestPaths:
                toReturn = min(self.shortestPaths.setdefault(
                    (self.users[userid].currentSpace, sid), \
                    pathTuple(shortestPath(self.spacesGraph, self.users[userid].currentSpace, sid))) \
                    for sid in self.events[eventid].spaces)
                return len(toReturn)
            else:
                return min(self.shortestPaths[(self.users[userid].currentSpace, sid)][0] \
                        for sid in self.events[eventid].spaces)

    # TODO: time for the duration? 
    def _enoughSpaceForUser(self, userid, eventid, time):
        # check the user type
        # check the capacity of the room
        occ = self.events[eventid].occupancy
        cap = self.events[eventid].capacity
        type = self.users[userid].type
        userTypeCapacity = occ[type] < cap[type]['hi'] or cap[type]['hi'] == -1
        spaces = filter(lambda sid: not self.spaces[sid].isFull(time), self.events[eventid].spaces)
        return userTypeCapacity and spaces 
                

    # TODO: time for the duration? 
    def _getEventDetailsForUser(self, userid, eventid, time):
        # TODO: change delta? 
        delta = lambda : int(round(np.random.exponential(3))) * np.random.choice([-1, 1])
        # startTime = event.startTime + datetime.timedelta(minutes=delta())
        endTime = self.events[eventid].endTime + datetime.timedelta(minutes=delta())
        spaces = filter(lambda sid: not self.spaces[sid].isFull(time), self.events[eventid].spaces)
        space = spaces[np.random.choice(len(spaces))]
        return endTime, space

    def _moveUserTowardsSpace(self, userid, spaceid, time):
        path = self.shortestPaths[(self.users[userid].currentSpace, spaceid)][1]
        minute = datetime.timedelta(minutes=1)
        second = datetime.timedelta(seconds=1)

        for spaceid in path[1:-1]:
            # TODO: change the amount of time it takes to go between adjacent rooms
            self.updateTrajectories(userid, spaceid, time, time + minute - second)
            self.spaces[spaceid].insertOccupancy(time, time + minute)
            time += minute
        return time

    def _userAttendsEvent(self, userid, spaceid, startTime, endTime):
        # print(str(startTime), str(endTime))
        endTime = max(startTime + datetime.timedelta(seconds=1), endTime)
        self.spaces[spaceid].insertOccupancy(startTime, endTime)
        self.updateTrajectories(userid, spaceid, startTime, endTime)
        
def createTrajectories(startTime, endTime, src, dest):
    print('Creating Trajectories')

    if showProfiler:
        pr = cProfile.Profile()
        pr.enable()

    mData = TrajectoriesData(startTime, endTime, src, dest)

    print('... Generating User Trajectories')
    mData.generateUserTrajectories()

    if showProfiler:
        pr.disable()

    ps = pstats.Stats(pr).sort_stats('tottime').print_stats()
    print('... Saving Trajectories')
    mData.dump()


