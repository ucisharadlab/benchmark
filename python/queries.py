
class Queries(object):

    def __init__(self, runs, dataDir, queriesDir, startTime, numLocations, numSensors, timeDelta):
        self.runs = runs
        self.dataDir = dataDir
        self.queriesDir = queriesDir
        self.startTime = startTime
        self.numLocations = numLocations
        self.numSensors = numSensors
        self.timeDelta = timeDelta

    def generateQueries(self):
        self.query1()
        self.query2(self.numLocations)
        self.query3(self.timeDelta)
        self.query4(self.numSensors, self.timeDelta)
        self.query5(self.timeDelta)
        self.query6(self.numSensors, self.timeDelta)
        self.query7()
        self.query8()
        self.query9()
        self.query10(self.timeDelta)

    def query1(self):
        pass

    def query2(self, numLocations):
        pass

    def query3(self, timeDelta):
        pass

    def query4(self, numSensors, timeDelta):
        pass

    def query5(self, timeDelta):
        pass

    def query6(self, numSensors, timeDelta):
        pass

    def query7(self):
        pass

    def query8(self):
        pass

    def query9(self):
        pass

    def query10(self, timeDelta):
        pass

