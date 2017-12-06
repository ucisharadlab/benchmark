import json
from datetime import datetime
import sys

common = ["location", "infrastructureType", "infrastructure",
          "sensorType", "group", "platformType", "sensor", "platform", "user"]

def copyFiles(files, src, dest):
    for file in files:
        with open(src + file +".json", "r") as r:
            data = json.loads(r.read())
            strings = []
            for row in data:

                strings.append(str(row)
                               .replace('"', '\\"')
                               .replace("'", '"')
                               )

        with open(dest + file + ".adm", "w") as w:
            w.write("{}".format('\n'.join(strings)))


def copyObservations(src, dest, mapping):
    admFile = open("{}observation.{}.adm".format(dest,mapping), "w")
    with open(src + "observation.json", "r") as r:
        for line in r:
            line = line.strip()
            line = line.strip(",")

            if not line or line.startswith("[") or line.startswith("]"):
                continue
            observation = json.loads(line.strip())

            if mapping == 1:
                del observation['sensor']['infrastructure']
                del observation['sensor']['owner']
                del observation['sensor']['coverage']
                del observation['sensor']['sensorConfig']

            if mapping == 2:
                observation['sensorId'] = observation['sensor']['id']
                del observation['sensor']

            observation["timeStamp"] = \
                    "datetime('" + datetime.strptime(observation["timeStamp"], "%Y-%m-%d %H:%M:%S") \
                        .strftime("%Y-%m-%dT%H:%M:%SZ") + "')"
            observation = str(observation)\
                .replace('"' + observation["timeStamp"] + '"', observation["timeStamp"])\
                .replace('"', '\\"')\
                .replace("'", '"')
            admFile.write(observation + "\n")

    admFile.close()


def copySemanticObservations(src, dest, mapping):
    admFile = open("{}semanticObservation.{}.adm".format(dest,mapping), "w")
    with open(src + "semanticObservation.json", "r") as r:
        for line in r:
            line = line.strip()
            line = line.strip(",")

            if not line or line.startswith("[") or line.startswith("]"):
                continue
            observation = json.loads(line.strip())

            if mapping == 1:
                observation['virtualSensorId'] = observation['virtualSensor']['id']
                del observation['virtualSensor']
                try:
                    del observation['semanticEntity']['geometry']
                except KeyError:
                    pass

            if mapping == 2:
                observation['semanticEntityId'] = observation['semanticEntity']['id']
                observation['virtualSensorId'] = observation['virtualSensor']['id']
                observation['typeId'] = observation['type_']['id']
                del observation['virtualSensor']
                del observation['semanticEntity']
                del observation['type_']

            observation["timeStamp"] = \
                    "datetime('" + datetime.strptime(observation["timeStamp"], "%Y-%m-%d %H:%M:%S") \
                        .strftime("%Y-%m-%dT%H:%M:%SZ") + "')"
            observation = str(observation)\
                .replace('"' + observation["timeStamp"] + '"', observation["timeStamp"])\
                .replace('"', '\\"')\
                .replace("'", '"')
            admFile.write(observation + "\n")

    admFile.close()


if __name__ == "__main__":

    mapping = int(sys.argv[1])
    copyObservations("data/", "data/", mapping)
    copySemanticObservations("data/", "data/", mapping)
