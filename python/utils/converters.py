import json
import csv
import sys

def copyObservationsM2(src, dest):
    #wifiFile = open("{}wifiobservation.csv".format(dest), "w")
    #wemoFile = open("{}wemoobservation.csv".format(dest), "w")
    thermoFile = open("{}thermometerobservation.csv".format(dest), "w")

    #wifiCsv = csv.writer(wifiFile)
    #wemoCsv = csv.writer(wemoFile)
    thermoCsv = csv.writer(thermoFile)

    #wifiCsv.writerow(["id", "clientId", "timeStamp", "sensor_id"])
    #wemoCsv.writerow(["id", "currentMilliWatts", "onTodaySeconds", "timeStamp", "sensor_id"])
    thermoCsv.writerow(["id", "temperature", "timeStamp", "sensor_id"])

    count = 0
    with open(src + "observation.json", "r") as r:
        for line in r:
            line = line.strip()
            line = line.strip(",")

            if not line or line.startswith("[") or line.startswith("]"):
                continue
            observation = json.loads(line.strip())

            #if observation["sensor"]["type_"]["id"] == "WiFiAP":
            #    wifiCsv.writerow([observation["id"], observation["payload"]["clientId"], observation["timeStamp"], observation["sensor"]["id"]])

            #if observation["sensor"]["type_"]["id"] == "WeMo":
            #    wemoCsv.writerow([observation["id"], observation["payload"]["currentMilliWatts"], observation["payload"]["onTodaySeconds"],
            #                      observation["timeStamp"], observation["sensor"]["id"]])

            if observation["sensor"]["type_"]["id"] == "Thermometer":
                thermoCsv.writerow([observation["id"], observation["payload"]["temperature"], observation["timeStamp"], observation["sensor"]["id"]])

            if count % 100000 == 0:
                print ("Observation Modified ", count)
            count += 1

    #wifiFile.close()
    #wemoFile.close()
    thermoFile.close()


def copySemanticObservationsM2(src, dest):
    presenceFile = open("{}presence.csv".format(dest), "w")
    occupancyFile = open("{}occupancy.csv".format(dest), "w")

    presenceCsv = csv.writer(presenceFile)
    occupancyCsv = csv.writer(occupancyFile)

    presenceCsv.writerow(["id", "semantic_entity_id", "location", "timeStamp", "virtual_sensor_id"])
    occupancyCsv.writerow(["id", "semantic_entity_id", "occupancy", "timeStamp", "virtual_sensor_id"])

    count = 0
    with open(src + "semanticObservation.json", "r") as r:
        for line in r:
            line = line.strip()
            line = line.strip(",")

            if not line or line.startswith("[") or line.startswith("]"):
                continue
            observation = json.loads(line.strip())

            if observation["type_"]["id"] == "presence":
                presenceCsv.writerow([observation["id"], observation["semanticEntity"]["id"], observation["payload"]["location"],
                                      observation["timeStamp"], observation["virtualSensor"]["id"]])

            elif observation["type_"]["id"] == "occupancy":
                occupancyCsv.writerow([observation["id"], observation["semanticEntity"]["id"], observation["payload"]["occupancy"],
                                      observation["timeStamp"], observation["virtualSensor"]["id"]])

            if count % 100000 == 0:
                print ("S Observation Modified ", count)
            count += 1

    presenceFile.close()
    occupancyFile.close()


if __name__ == "__main__":

    mapping = int(sys.argv[1])
    copyObservationsM2("/mnt/data/sdb/peeyushg/benchmark/datasets/large/", "/mnt/data/sdb/peeyushg/benchmark/datasets/large/")
    #copySemanticObservationsM2("/mnt/data/sdb/peeyushg/benchmark/datasets/large/", "/mnt/data/sdb/peeyushg/benchmark/datasets/large/")
