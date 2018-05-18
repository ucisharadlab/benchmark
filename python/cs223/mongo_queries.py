import csv
import sys
import random
import datetime

Q1 = """
{
  "find": "Sensor",
  "filter": {
    "_id": "%s"
  },
  "projection": {
    "name": 1,
    "_id": 0
  }
}"""

Q2 = """
{
  "find": "Sensor",
  "filter": {
    "type_.name": "%s",
    "coverage.id": {
      "$in": [%s]
    }
  },
  "projection": {
    "name": 1,
    "_id": 1
  }
}"""

Q3 = """
{
    "find" : "Observation",
    "filter" : {
        "timeStamp" : {
            "$gt" : ISODate("%s"),
            "$lt" : ISODate("%s")
        },
        "sensor.id" : "%s"
    },
    "projection" : {
        "timeStamp" : 1,
        "_id" : 0,
        "payload" : 1,
        "sensor.id" : 1
    }
}
"""

Q4 = """
{
    "find" : "Observation",
    "filter" : {
        "timeStamp" : {
            "$gt" : ISODate("%s"),
            "$lt" : ISODate("%s")
        },
        "sensor.id" : {
            "$in" : [%s]
        }
    },
    "projection" : {
        "timeStamp" : 1,
        "_id" : 0,
        "sensor.id" : 1,
        "payload.temperature" : 1
    }
}
"""

Q5 = """
{
    "find" : "Observation",
    "filter" : {
        "sensor.type_.name" : "%s",
        "timeStamp" : {
            "$gt" : ISODate("%s"),
            "$lt" : ISODate("%s")
        },
        "payload.%s" : {
            "$gt" : %s,
            "$lt" : %s
        }
    },
    "projection" : {
        "timeStamp" : 1,
        "_id" : 0,
        "sensor.id" : 1,
        "payload" : 1
    }
}
"""

Q6 = """
{
    "aggregate" : "Observation",
    "pipeline" : [
        {
            "$match" : {
                "timeStamp" : {
                    "$gt" : ISODate("%s"),
                    "$lt" : ISODate("%s")
                },
                "sensor.id" : {
                    "$in" : [%s]
                }
            }
        },
        {
            "$project" : {
                "_id" : 0,
                "sensor.id" : 1,
                "date" : {
                    "$dateToString" : {
                        "format" : "%Y-%m-%d",
                        "date" : "$timeStamp"
                    }
                }
            }
        },
        {
            "$group" : {
                "_id" : {
                    "sensorId" : "$sensor.id",
                    "date" : "$date"
                },
                "count" : {
                    "$sum" : 1
                }
            }
        },
        {
            "$group" : {
                "_id" : {
                    "sensorId" : "$_id.sensorId"
                },
                "averagePerDay" : {
                    "$avg" : "$count"
                }
            }
        }
    ],
    "cursor" : {

    }
}
"""

# Q7 = """
# SELECT u.name
# FROM PRESENCE s1, PRESENCE s2, USERS u
# WHERE date_trunc('day', s1.timeStamp) = '{}' AND date_trunc('day', s2.timeStamp) = '{}' AND s1.semantic_entity_id = s2.semantic_entity_id
# AND s1.location = '{}' AND s2.location = '{}' AND s1.timeStamp < s2.timeStamp AND s1.semantic_entity_id = u.id
# """
#
# Q8 = """
# SELECT u.name, s1.location
# FROM PRESENCE s1, PRESENCE s2, USERS u
# WHERE date_trunc('day', s1.timeStamp) = '{}' AND s2.timeStamp = s1.timeStamp AND s1.semantic_entity_id = '{}'
# AND s1.semantic_entity_id != s2.semantic_entity_id AND s2.semantic_entity_id = u.id AND s1.location = s2.location
# """

Q9 = """
{
    "aggregate" : "SemanticObservation",
    "pipeline" : [
        {
            "$lookup" : {
                "from" : "Infrastructure",
                "localField" : "payload.location",
                "foreignField" : "_id",
                "as" : "infra"
            }
        },
        {
            "$unwind" : "$infra"
        },
        {
            "$match" : {
                "infra.type_.name" : "%s",
                "semanticEntity.id" : "%s"
            }
        },
        {
            "$project" : {
                "_id" : 0,
                "date" : {
                    "$dateToString" : {
                        "format" : "%Y-%m-%d",
                        "date" : "$timeStamp"
                    }
                }
            }
        },
        {
            "$group" : {
                "_id" : {
                    "date" : "$date"
                },
                "count" : {
                    "$sum" : 1
                }
            }
        },
        {
            "$group" : {
                "_id" : null,
                "averageMinsPerDay" : {
                    "$avg" : "$count"
                }
            }
        }
    ],
    "cursor" : {

    }
}
"""

Q10 = """
{
    "aggregate" : "SemanticObservation",
    "pipeline" : [
        {
            "$match" : {
                "timeStamp" : {
                    "$gt" : ISODate("%s"),
                    "$lt" : ISODate("%s")
                },
                "type_.name" : "occupancy"
            }
        },
        {
            "$sort" : {
                "semanticEntity.id" : 1,
                "timeStamp" : 1
            }
        },
        {
            "$project" : {
                "_id" : 0,
                "timeStamp" : 1,
                "semanticEntity.name" : 1,
                "payload.occupancy" : 1
            }
        }
    ],
    "cursor" : {

    }
}
"""


def getRandomTimeStamp():
    start = "2017-11-08 00:00:00"
    maxDays = 20

    start = datetime.datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
    t1 = start + datetime.timedelta(minutes=random.randrange(maxDays * 24 * 60))
    return t1.strftime('%Y-%m-%dT%H:%M:%SZ')


def addDayToTime(start):
    start = datetime.datetime.strptime(start, "%Y-%m-%dT%H:%M:%SZ")
    t1 = start + datetime.timedelta(days=1)
    return t1.strftime('%Y-%m-%dT%H:%M:%SZ')


def generateTimeQueries(dir):

    queries = []

    # Query 1
    with open(dir+'query1.txt', 'r') as csvfile:
        next(csvfile)
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in spamreader:
            queries.append((getRandomTimeStamp(), Q1 % row[1:]))
    print(queries[-1])
    print()

    # Query 2
    with open(dir + 'query2.txt', 'r') as csvfile:
        next(csvfile)
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in spamreader:
            params = row[1:]
            params[1] = ','.join(map(lambda x: '"{}"'.format(x), params[1].split(";")))
            queries.append((getRandomTimeStamp(), Q2 % params))
        print(queries[-1])
        print()

    # # Query 7
    # with open(dir + 'query7.txt', 'r') as csvfile:
    #     next(csvfile)
    #     spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
    #     for row in spamreader:
    #         params = [row[3], row[3], row[1], row[2]]
    #         queries.append((addDayToTime(row[3]), Q7.format(*(params))))
    # print(queries[-1])
    # print()
    #
    # # Query 8
    # with open(dir + 'query8.txt', 'r') as csvfile:
    #     next(csvfile)
    #     spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
    #     for row in spamreader:
    #         params = [row[2], row[1]]
    #         queries.append((addDayToTime(row[2]), Q8.format(*(params))))
    # print(queries[-1])
    # print()

    # Query 9
    with open(dir + 'query9.txt', 'r') as csvfile:
        next(csvfile)
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in spamreader:
            params = [row[2], row[1]]
            queries.append((getRandomTimeStamp(), Q9 % params))
    print(queries[-1])
    print()

    # Query 10
    with open(dir + 'query10.txt', 'r') as csvfile:
        next(csvfile)
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in spamreader:
            queries.append((row[2], Q10 % row[1:] ))
    print(queries[-1])
    print()

    # Query 5
    with open(dir + 'query5.txt', 'r') as csvfile:
        next(csvfile)
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in spamreader:
            params = [row[1], row[2], row[3], row[4], row[6], row[7]]
            queries.append((row[3],Q5 % params))
    print(queries[-1])
    print()

    # Query 3
    with open(dir + 'query3.txt', 'r') as csvfile:
        next(csvfile)
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in spamreader:
            params = [row[4], row[5], row[3]]
            queries.append((row[5], Q3 % params))

    print(queries[-1])
    print()

    # Query 4
    with open(dir + 'query4.txt', 'r') as csvfile:
        next(csvfile)
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in spamreader:
            params = [row[4], row[5], row[3]]
            params[2] = ','.join(map(lambda x: '"{}"'.format(x), params[2].split(";")))
            queries.append((row[5], Q4 % params))
        print(queries[-1])
        print()

    # Query 6
    with open(dir + 'query6.txt', 'r') as csvfile:
        next(csvfile)
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in spamreader:
            params = [ row[4], row[5], row[3]]
            params[2] = ','.join(map(lambda x: '"{}"'.format(x), params[2].split(";")))
            queries.append((row[5], Q6 % params))
        print(queries[-1])
        print()

    queries.sort()

    with open(dir + 'mongodb_queries.txt', 'w') as csvfile:
        writer = csv.writer(csvfile, delimiter=',', quotechar="`")
        writer.writerows(queries)


if __name__ == "__main__":
    dir = sys.argv[1]
    generateTimeQueries(dir)

