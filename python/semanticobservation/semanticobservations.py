import json
import uuid
import datetime

import presence, occupancy
from utils.helper import jsonToCsvLine, jsonToCsvOccupancy, toUTC, jsonToCsvOccupancyFromDict, jsonToCsvPresenceLine


def createObservations(dt, end, step, dataDir, outputDir):

    line = None
    finalObj = open(outputDir + 'semanticObservation.json', 'w')
    finalObj.write("[\n")

    presence.createPresence(dt, end, step, outputDir)
    prObj = open("data/presenceData.json")
    for line in prObj:
        finalObj.write(line + ",\n")
    prObj.close()

    occupancy.createOccupancy(dt, end, step, outputDir)
    ocObj = open("data/occupancyData.json")
    for line in ocObj:
        finalObj.write(line + ",\n")
    ocObj.close()

    line = json.loads(line)
    line['id'] = str(uuid.uuid4())

    finalObj.write(json.dumps(line))
    finalObj.write("\n]")

    finalObj.close()


def createIntelligentObservations(start, end, origDays, extendDays, origSpeed, extendSpeed, speedScaleNoise,
                               timeScaleNoise, dataDir, outputDir):

    # Generating Floor Level Presence

    with open(outputDir+'user.json') as data_file:
        users = json.load(data_file)

    presence.createPresence(len(users), 200, start, end, outputDir)
    line = None
    finalObj = open(outputDir + 'presence.json', 'w')
    finalObj.write("[\n")
    prObj = open("data/presenceData.json")
    for line in prObj:
        finalObj.write(line + ",\n")
    prObj.close()

    line = json.loads(line)
    line['id'] += line['id']

    finalObj.write(json.dumps(line))
    finalObj.write("\n]")

    finalObj.close()
    jsonToCsvPresenceLine(outputDir + 'presence.json', outputDir + 'presence.csv')

    # ------------------------------

    # Generating Building Level Occupancy

    count = occupancy.createIntelligentOccupancy(origDays, extendDays, origSpeed, extendSpeed, speedScaleNoise,
                               timeScaleNoise, outputDir)

    finalObj = open(outputDir + 'occupancy.json', 'w')
    finalObj.write("[\n")

    ocObj = open("data/occupancyData.json")
    for line in ocObj:
        finalObj.write(line + ",\n")
    ocObj.close()

    # --------------------------------

    # Generating Floor Level Occupancy

    with open(outputDir+'presence.json') as data_file:
        presenceData = json.load(data_file)

    with open(dataDir + 'mobileland.json') as data_file:
        rooms = json.load(data_file)

    numRooms = len(rooms)
    occupancyArray = dict({rooms[i]['name']: [0]*24*12*30 for i in range(numRooms)})

    startUTC = toUTC(start)
    for p in presenceData:
        start = p['start_timestamp']
        end = p['end_timestamp']
        location = p['location']

        while start < end:
            x = (start-startUTC)/300
            occupancyArray[location][x] += 1
            start += 60*5

    count += 1
    object = None
    for k, v in occupancyArray.iteritems():
        dateCount = 0
        for iv in v:
            object = {
                "id": count,
                "location": k,
                "timeStamp": datetime.datetime.utcfromtimestamp(startUTC + dateCount*300 - 7*3600).strftime('%Y-%m-%d %H:%M:%S'),
                "occupancy": iv,
                "green_badges": iv,
                "yellow_badges": 0,
                "red_badges": 0
            }
            dateCount += 1
            count += 1

            finalObj.write(json.dumps(object) + ",\n")

    object['id'] += object['id']

    finalObj.write(json.dumps(object))
    finalObj.write("\n]")
    finalObj.close()

    with open(outputDir + 'occupancy.json', 'r') as fp:
        occupancyData = json.load(fp)
    occupancyData  = sorted(occupancyData, key=lambda x: datetime.datetime.strptime(x['timeStamp'], '%Y-%m-%d %H:%M:%S'))

    jsonToCsvOccupancyFromDict(occupancyData, outputDir + 'occupancy.csv')
