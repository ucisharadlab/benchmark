import csv
import sys
import random
import datetime

Q1 = """
SELECT name FROM Sensor WHERE id = "{}"
"""

Q2 = """
SELECT sen.name 
FROM SENSOR sen, SENSOR_TYPE st, COVERAGE_INFRASTRUCTURE ci 
WHERE sen.SENSOR_TYPE_ID=st.id AND st.name='{}' AND sen.id=ci.SENSOR_ID AND ci.INFRASTRUCTURE_ID=ANY(array[{}])
"""

Q3 = """
SELECT timeStamp, sensor.id, payload 
FROM Observation 
WHERE timeStamp >= "{}" AND timeStamp <= "{}" AND sensor.id="{}"
"""

Q4 = """
SELECT timeStamp, sensor.id, payload 
FROM Observation 
WHERE timeStamp >= "{}" AND timeStamp <= "{}" AND sensor.id IN [ {} ] 
"""

Q5 = """
SELECT timeStamp, sensor.id, payload 
FROM Observation 
WHERE timeStamp >= "{}" AND timeStamp <= "{}" AND sensor.type_.name = "{}" AND payload.{} >= {} AND payload.{} <= {}
"""

Q6 = """
SELECT obs.id , AVG(obs.count) 
FROM (SELECT sensor.id , DATE_FORMAT_STR(timeStamp, '1111-11-11'), count(*)  AS count 
      FROM Observation WHERE timeStamp >= "{}" AND timeStamp <= "{}" AND sensor.id IN [ {} ] 
      GROUP BY sensor.id, DATE_FORMAT_STR(timeStamp, '1111-11-11')) AS obs 
GROUP BY obs.id
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
SELECT Avg(timeSpent) as avgTimeSpent FROM 
	(SELECT date_trunc('day', so.timeStamp), count(*)*10 as timeSpent 
         FROM PRESENCE so, Infrastructure infra, Infrastructure_Type infraType 
         WHERE so.location = infra.id AND infra.INFRASTRUCTURE_TYPE_ID = infraType.id AND infraType.name = '{}' AND so.semantic_entity_id = '{}' 
         GROUP BY  date_trunc('day', so.timeStamp)) AS timeSpentPerDay
"""

Q10 = """
SELECT infra.name, so.timeStamp, so.occupancy 
FROM OCCUPANCY so, INFRASTRUCTURE infra 
WHERE so.timeStamp > '{}' AND so.timeStamp < '{}' AND so.semantic_entity_id = infra.id 
ORDER BY so.semantic_entity_id, so.timeStamp

SELECT infra.name, (SELECT so.timeStamp, so.payload.occupancy 
                    FROM SemanticObservation so 
                    WHERE so.timeStamp > "{}" AND so.timeStamp < "{}" AND so.type_.name = "occupancy" AND so.semanticEntity.id = infra.id ORDER BY so.semanticEntity.id, so.timeStamp) AS histogram 
FROM Infrastructure infra
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
            queries.append((getRandomTimeStamp(), Q1.format(*(row[1:]))))
    print(queries[-1])
    print()

    # Query 2
    with open(dir + 'query2.txt', 'r') as csvfile:
        next(csvfile)
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in spamreader:
            params = row[1:]
            params[1] = ','.join(map(lambda x: "'{}'".format(x), params[1].split(";")))
            queries.append((getRandomTimeStamp(), Q2.format(*(params))))
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
            queries.append((getRandomTimeStamp(), Q9.format(*(params))))
    print(queries[-1])
    print()

    # Query 10
    with open(dir + 'query10.txt', 'r') as csvfile:
        next(csvfile)
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in spamreader:
            queries.append((row[2], Q10.format(*(row[1:]))))
    print(queries[-1])
    print()

    # Query 5
    with open(dir + 'query5.txt', 'r') as csvfile:
        next(csvfile)
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in spamreader:
            params = [row[4], row[1], row[2], row[3], row[4], row[6], row[4], row[7]]
            queries.append((row[3], Q5.format(*(params))))
    print(queries[-1])
    print()

    # Query 3
    with open(dir + 'query3.txt', 'r') as csvfile:
        next(csvfile)
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in spamreader:
            params = [row[1], row[2], row[4], row[5], row[3]]
            queries.append((row[5], Q3.format(*params)))

    print(queries[-1])
    print()

    # Query 4
    with open(dir + 'query4.txt', 'r') as csvfile:
        next(csvfile)
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in spamreader:
            params = [row[1], row[2], row[4], row[5], row[3]]
            params[4] = ','.join(map(lambda x: "'{}'".format(x), params[4].split(";")))
            queries.append((row[5], Q4.format(*(params))))
        print(queries[-1])
        print()

    # Query 6
    with open(dir + 'query6.txt', 'r') as csvfile:
        next(csvfile)
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in spamreader:
            params = [row[2], row[4], row[5], row[3]]
            params[3] = ','.join(map(lambda x: "'{}'".format(x), params[3].split(";")))
            queries.append((row[5], Q6.format(*(params))))
        print(queries[-1])
        print()

    queries.sort()

    with open(dir + 'queries.txt', 'w') as csvfile:
        writer = csv.writer(csvfile, delimiter=',', quotechar="`")
        writer.writerows(queries)


if __name__ == "__main__":
    dir = sys.argv[1]
    generateTimeQueries(dir)
