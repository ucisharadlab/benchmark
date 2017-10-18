import helper
from helper import Helper

class TrajectoryScale(object):

    def __init__(self):
        self.outputFilename = "simulatedObs.json"
        self.helper = Helper()

    def setOutputFileName(self, outputFileName):
        self.outputFilename = outputFileName

    def generatePersonPaths(self, personNum, timeInterval):
        rand = Random()

        traj = TrajectoryParser1()
        traj.parseData(self.outputFilename)
        connectMap = traj.getConnectMap()

        startTime = "2017-07-11 00:00:00"
        obsNumPerDay = 24 * 60 / timeInterval
        keys = connectMap.keySet()
        keyArray = keys.toArray(String[keys.size()])
        keySize = keyArray.length

        jsonWriter = None
        try:
            jsonWriter = JsonWriter(FileWriter(self.outputFilename))
            jsonWriter.setIndent("  ")
            jsonWriter.beginArray()
            for i in range(1, personNum+1):
                jsonWriter.beginObject()
                jsonWriter.name("id")
                jsonWriter.value("Person" + i)
                jsonWriter.name("path")
                jsonWriter.beginArray()
                keyIndex = rand.nextInt(keySize)
                area = keyArray[keyIndex]
                timestamp = startTime

                for j in range(obsNumPerDay):

                    jsonWriter.beginObject()
                    jsonWriter.name("area")
                    jsonWriter.value(area)
                    jsonWriter.name("timestamp")
                    jsonWriter.value(timestamp)
                    jsonWriter.endObject()


                    timestamp = helper.increaseTime(timestamp, obsNumPerDay)

                    connectsOfArea = connectMap.get(area)
                    connectsOfArea.add(area)
                    connectSize = connectsOfArea.size()
                    area = connectsOfArea.get(rand.nextInt(connectSize))

            jsonWriter.endArray()
            jsonWriter.endObject()


            jsonWriter.endArray()
        except Exception as e:
            print("IO error")
        finally:
            try:
                jsonWriter.close()
            except Exception as e:
                print("IO error")

def generateWifiAP(self, timeInterval):
    traj = TrajectoryParser2()
    traj.parseData(self.outputFilename)
    wifiMap = traj.getWifiMap()
    areas = traj.getAreas()
    Collections.sort(areas)

    startTime = "2017-07-11 00:00:00"
    obsNumPerDay = 24 * 60 / timeInterval

    timestamp = startTime

    jsonWriter = None
    try:
        jsonWriter = JsonWriter(FileWriter("SimulatedData/wifiAP.json"))
        jsonWriter.setIndent("  ")
        jsonWriter.beginArray()
        for i in range(obsNumPerDay):
            for area in areas:
                users = wifiMap.get(area).get(timestamp)
                if users != None:
                    jsonWriter.beginObject()
                    jsonWriter.name("id")
                    jsonWriter.value(area)
                    jsonWriter.name("timestamp")
                    jsonWriter.value(timestamp)
                    jsonWriter.name("users")
                    jsonWriter.beginArray()
                    l = users.size()
                    j = 0
                    while j<l:
                        j+=1
                        jsonWriter.value(users.get(j))

                    jsonWriter.endArray()
                    jsonWriter.endObject()
            timestamp = helper.increaseTime(timestamp, obsNumPerDay)
        jsonWriter.endArray()
    except Exception as e:
        print("IO error")
    finally:
        try:
            jsonWriter.close()
        except Exception as e:
            print("IO error")
