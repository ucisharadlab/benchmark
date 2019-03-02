import json
from datetime import datetime
import sys
import csv

from helper import toUTC

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
    if mapping == 1:
        return

    wifiCSVFile = open("{}wifiobservation.csv".format(dest), "w")
    thermometerCSVFile = open("{}thermometerobservation.csv".format(dest), "w")
    wemoCSVFile = open("{}wemoobservation.csv".format(dest), "w")

    wifiWriter = csv.writer(wifiCSVFile, quotechar="\"")
    thermometerWriter = csv.writer(thermometerCSVFile, quotechar="\"")
    wemoWriter = csv.writer(wemoCSVFile, quotechar="\"")

    count = 0
    with open(src + "observation.json", "r") as r:
        for line in r:

            if count == 0:
                wifiWriter.writerow(["id","clientid","timestamp","sensor_id"])
                wemoWriter.writerow(["id","currentmilliwatts","ontodayseconds","timestamp","sensor_id"])
                thermometerWriter.writerow(["id","temperature","timestamp","sensor_id"])
                count += 1

            line = line.strip()
            line = line.strip(",")

            if not line or line.startswith("[") or line.startswith("]"):
                continue
            observation = json.loads(line.strip())

            if observation['sensor']['type_']['id'] == 'WiFiAP':
                wifiWriter.writerow([observation['id'], observation['payload']['clientId'],
                                     toUTC(datetime.strptime(observation['timeStamp'], "%Y-%m-%d %H:%M:%S")),
                                     observation['sensor']['id']])
            elif observation['sensor']['type_']['id'] == 'WeMo':
                wemoWriter.writerow([observation['id'], observation['payload']['currentMilliWatts'], observation['payload']['currentMilliWatts'],
                                     toUTC(datetime.strptime(observation['timeStamp'], "%Y-%m-%d %H:%M:%S")),
                                     observation['sensor']['id']])
            elif observation['sensor']['type_']['id'] == 'Thermometer':
                thermometerWriter.writerow([observation['id'], observation['payload']['temperature'],
                                     toUTC(datetime.strptime(observation['timeStamp'], "%Y-%m-%d %H:%M:%S")),
                                     observation['sensor']['id']])

            if count % 100000 == 0:
                print ("Observation Modified ", count)
            count += 1

    wifiCSVFile.close()
    wemoCSVFile.close()
    thermometerCSVFile.close()


def copySemanticObservations(src, dest, mapping):
    if mapping == 1:
        return

    presenceCSVFile = open("{}presence.csv".format(dest), "w")
    occupancyCSVFile = open("{}occupancy.csv".format(dest), "w")

    presenceWriter = csv.writer(presenceCSVFile, quotechar="\"")
    occupancyWriter = csv.writer(occupancyCSVFile, quotechar="\"")

    count = 0
    with open(src + "semanticObservation.json", "r") as r:
        for line in r:

            if count == 0:
                presenceWriter.writerow(["id","semantic_entity_id","location","timestamp","virtual_sensor_id"])
                occupancyWriter.writerow(["id","semantic_entity_id","occupancy","timestamp","virtual_sensor_id"])
                count += 1

            line = line.strip()
            line = line.strip(",")

            if not line or line.startswith("[") or line.startswith("]"):
                continue
            observation = json.loads(line.strip())

            if observation["type_"]["id"] == 'presence':
                presenceWriter.writerow([observation['id'], observation['semanticEntity']['id'], observation['payload']['location'],
                                     toUTC(datetime.strptime(observation['timeStamp'], "%Y-%m-%d %H:%M:%S")),
                                     observation['virtualSensor']['id']])
            elif observation["type_"]["id"] == 'occupancy':
                occupancyWriter.writerow([observation['id'], observation['semanticEntity']['id'], observation['payload']['occupancy'],
                                     toUTC(datetime.strptime(observation['timeStamp'], "%Y-%m-%d %H:%M:%S")),
                                     observation['virtualSensor']['id']])
            if count % 100000 == 0:
                print ("S Observation Modified ", count)
            count += 1

    presenceCSVFile.close()
    occupancyCSVFile.close()

if __name__ == "__main__":

    mapping = int(sys.argv[1])
    copyObservations("data/", "data/", mapping)
    copySemanticObservations("data/", "data/", mapping)
