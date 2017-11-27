import json
import uuid

import presence, occupancy


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


def createIntelligentObservations(origDays, extendDays, origSpeed, extendSpeed, speedScaleNoise,
                               timeScaleNoise, dataDir, outputDir):

    occupancy.createIntelligentOccupancy(origDays, extendDays, origSpeed, extendSpeed, speedScaleNoise,
                               timeScaleNoise, outputDir)

    line = None
    finalObj = open(outputDir + 'semanticObservation.json', 'w')
    finalObj.write("[\n")

    # Presence Already Created By WiFi Data
    prObj = open("data/presenceData.json")
    for line in prObj:
        finalObj.write(line + ",\n")
    prObj.close()


    ocObj = open("data/occupancyData.json")
    for line in ocObj:
        finalObj.write(line + ",\n")
    ocObj.close()

    line = json.loads(line)
    line['id'] = str(uuid.uuid4())

    finalObj.write(json.dumps(line))
    finalObj.write("\n]")

    finalObj.close()
