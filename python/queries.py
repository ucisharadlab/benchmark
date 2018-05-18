import json
import random
import numpy as np
import datetime

SEN_TYPES = ["WiFiAP", "WeMo", "Thermometer"]
SO_TYPES = ["presence", "occupancy"]


class Queries(object):

    def __init__(self, runs, dataDir, queriesDir, startTime, maxDays, numLocations, numSensors, timeDelta):
        self.runs = runs
        self.dataDir = dataDir
        self.queriesDir = queriesDir
        self.startTime = startTime
        self.maxDays = maxDays
        self.numLocations = numLocations
        self.numSensors = numSensors
        self.timeDelta = timeDelta

        self.init()

    def init(self):
        with open(self.dataDir + 'sensor.json') as data_file:
            self.sensors = json.load(data_file)

        with open(self.dataDir + 'infrastructure.json') as data_file:
            self.infra = json.load(data_file)

        with open(self.dataDir + 'user.json') as data_file:
            self.users = json.load(data_file)

        with open(self.dataDir + 'infrastructureType.json') as data_file:
            self.infraTypes = json.load(data_file)

    def generateQueries(self):
        print ("Generating Queries")

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

    def writeQueries(self, qNum, header, queries):
        with open(self.queriesDir + 'query{}.txt'.format(qNum), "w") as qfile:
            qfile.write(header+"\n")
            qfile.writelines(queries)

    def randomTimeRange(self, start, maxDays, rangeDays):
        start = datetime.datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
        t1 = start + datetime.timedelta(minutes=random.randrange(maxDays*24*60))
        t2 = t1 + datetime.timedelta(days=rangeDays)
        return t1.strftime('%Y-%m-%dT%H:%M:%SZ'), t2.strftime('%Y-%m-%dT%H:%M:%SZ')

    def randomDate(self, start, maxDays):
        start = datetime.datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
        t1 = start + datetime.timedelta(minutes=random.randrange(maxDays * 24 * 60))
        return t1.strftime('%Y-%m-%dT00:00:00Z')

    def query1(self):
        queries = []
        for i in range(self.runs):
            sensorId = self.sensors[random.randint(0, len(self.sensors)-1)]['id']
            queries.append("{},{}\n".format(i, sensorId))

        self.writeQueries(1, "# time(s), sensorId", queries)

    def query2(self, numLocations):
        queries = []
        for i in range(self.runs):
            locations = [self.infra[x]['id'] for x in np.random.choice(len(self.infra), numLocations, replace=False)]
            sensorType = SEN_TYPES[random.randint(0, 2)]
            queries.append("{},{},{}\n".format(i, sensorType, ';'.join(locations)))

        self.writeQueries(2, "# time(s), sensorTypeName, locationIds(List)", queries)

    def query3(self, timeDelta):
        queries = []
        for i in range(self.runs):
            sensor = self.sensors[random.randint(0, len(self.sensors) - 1)]
            t1, t2 = self.randomTimeRange(self.startTime, self.maxDays, timeDelta)
            if sensor['type_']['name'] == "WeMo":
                queries.append("{},currentmilliwatts,{},{},{},{}\n".format(i,sensor['type_']['name'], sensor['id'], t1, t2))
            if sensor['type_']['name'] == "WiFiAP":
                queries.append("{},clientid,{},{},{},{}\n".format(i,sensor['type_']['name'], sensor['id'], t1, t2))
            if sensor['type_']['name'] == "Thermometer":
                queries.append("{},temperature,{},{},{},{}\n".format(i,sensor['type_']['name'], sensor['id'], t1, t2))
        self.writeQueries(3, "# time(s), type_, sensorId, startTime, endTime", queries)

    def query4(self, numSensors, timeDelta):
        queries = []
        for i in range(int(self.runs/3)):
            sensors = list(filter(lambda x: x['type_']['name']=='WeMo', self.sensors))
            sensors = [self.sensors[x] for x in np.random.choice(len(sensors), numSensors, replace=False)]
            t1, t2 = self.randomTimeRange(self.startTime, self.maxDays, timeDelta)
            queries.append("{},currentmilliwatts,WeMo,{},{},{}\n".format(i, ';'.join(map(lambda x: x['id'], sensors)), t1, t2))

        for i in range(int(self.runs/3)):
            sensors = list(filter(lambda x: x['type_']['name']=='Thermometer', self.sensors))
            sensors = [self.sensors[x] for x in np.random.choice(len(sensors), numSensors, replace=False)]
            t1, t2 = self.randomTimeRange(self.startTime, self.maxDays, timeDelta)
            queries.append("{},temperature,Thermometer,{},{},{}\n".format(i, ';'.join(map(lambda x: x['id'], sensors)), t1, t2))

        for i in range(int(self.runs/3)):
            sensors = list(filter(lambda x: x['type_']['name']=='WiFiAP', self.sensors))
            sensors = [self.sensors[x] for x in np.random.choice(len(sensors), numSensors, replace=False)]
            t1, t2 = self.randomTimeRange(self.startTime, self.maxDays, timeDelta)
            queries.append("{},clientid,WiFiAP,{},{},{}\n".format(i, ';'.join(map(lambda x: x['id'], sensors)), t1, t2))

        self.writeQueries(4, "# time(s), sensorIds(List), startTime, endTime, type", queries)

    def query5(self, timeDelta):
        # TODO: Currently only for temperature sensors, change to multiple types
        queries = []
        for i in range(self.runs):
            p1 = random.randint(0, 50)
            p2 = random.randint(p1+1, 100)
            t1, t2 = self.randomTimeRange(self.startTime, self.maxDays, timeDelta)
            queries.append("{},Thermometer,{},{},temperature,INT,{},{}\n".format(i, t1, t2, p1, p2))

        self.writeQueries(5, "# time(s), sensorTypeName, startTime, endTime, payloadAttribute, type_(INT,DOUBLE,STRING), "
                             "startPayloadValue, endPayloadValue", queries)

    def query6(self, numSensors, timeDelta):
        queries = []
        for i in range(int(self.runs/3)):
            sensors = list(filter(lambda x: x['type_']['name']=='WeMo', self.sensors))
            sensors = [self.sensors[x] for x in np.random.choice(len(sensors), numSensors, replace=False)]
            t1, t2 = self.randomTimeRange(self.startTime, self.maxDays, timeDelta)
            queries.append("{},currentmilliwatts,WeMo,{},{},{}\n".format(i, ';'.join(map(lambda x: x['id'], sensors)), t1, t2))

        for i in range(int(self.runs/3)):
            sensors = list(filter(lambda x: x['type_']['name']=='Thermometer', self.sensors))
            sensors = [self.sensors[x] for x in np.random.choice(len(sensors), numSensors, replace=False)]
            t1, t2 = self.randomTimeRange(self.startTime, self.maxDays, timeDelta)
            queries.append("{},temperature,Thermometer,{},{},{}\n".format(i, ';'.join(map(lambda x: x['id'], sensors)), t1, t2))

        for i in range(int(self.runs/3)):
            sensors = list(filter(lambda x: x['type_']['name']=='WiFiAP', self.sensors))
            sensors = [self.sensors[x] for x in np.random.choice(len(sensors), numSensors, replace=False)]
            t1, t2 = self.randomTimeRange(self.startTime, self.maxDays, timeDelta)
            queries.append("{},clientid,WiFiAP,{},{},{}\n".format(i, ';'.join(map(lambda x: x['id'], sensors)), t1, t2))

        self.writeQueries(6, "# time(s), sensorIds(List), startTime, endTime", queries)

    def query7(self):
        queries = []
        for i in range(self.runs):
            startLocation, endLocation = [self.infra[x]['id'] for x in np.random.choice(len(self.infra), 2, replace=False)]
            date = self.randomDate(self.startTime, self.maxDays)
            queries.append("{},{},{},{}\n".format(i, startLocation, endLocation, date))

        self.writeQueries(7, "# time(s), startLocation, endLocation, date", queries)

    def query8(self):
        queries = []
        for i in range(self.runs):
            userId = self.users[random.randint(0, len(self.users) - 1)]['id']
            date = self.randomDate(self.startTime, self.maxDays)
            queries.append("{},{},{}\n".format(i, userId, date))

        self.writeQueries(8, "# time(s), userId, date", queries)

    def query9(self):
        queries = []
        for i in range(self.runs):
            userId = self.users[random.randint(0, len(self.users) - 1)]['id']
            infraTypeName = self.infraTypes[random.randint(0, len(self.infraTypes) - 1)]['name']
            queries.append("{},{},{}\n".format(i, userId, infraTypeName))

        self.writeQueries(9, "# time(s), userId, infraTypeName", queries)

    def query10(self, timeDelta):
        queries = []
        for i in range(self.runs):
            t1, t2 = self.randomTimeRange(self.startTime, self.maxDays, timeDelta)
            queries.append("{},{},{}\n".format(i, t1, t2))

        self.writeQueries(10, "# time(s), start, end", queries)


