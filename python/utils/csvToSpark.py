import json
from datetime import datetime
import sys
import csv


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

    wifiCSVFile = open("{}wifiobservation.csv".format(dest))
    thermometerCSVFile = open("{}thermometerobservation.csv".format(dest))
    wemoCSVFile = open("{}wemoobservation.csv".format(dest))

    spark_wifiCSVFile = open("{}spark_wifiobservation.csv".format(dest), "w")
    spark_thermometerCSVFile = open("{}spark_thermometerobservation.csv".format(dest), "w")
    spark_wemoCSVFile = open("{}spark_wemoobservation.csv".format(dest), "w")

    wifiWriter = csv.writer(spark_wifiCSVFile, quotechar="\"")
    thermometerWriter = csv.writer(spark_thermometerCSVFile, quotechar="\"")
    wemoWriter = csv.writer(spark_wemoCSVFile, quotechar="\"")

    count = 0
    for line in wifiCSVFile:
        if count == 0:
            wifiWriter.writerow(["id","clientid","timestamp","sensor_id"])
            # wemoWriter.writerow(["id","currentmilliwatts","ontodayseconds","timestamp","sensor_id"])
            # thermometerWriter.writerow(["id","temperature","timestamp","sensor_id"])
            count += 1
            continue

        line = line.strip()

        row = line.split(",")
        row[2] = datetime.fromtimestamp(int(row[2])/1000).strftime('%Y-%m-%d %H:%M:%S')
        wifiWriter.writerow(row)

        if count % 100000 == 0:
            print ("Observation Modified ", count)
        count += 1

    count = 0
    for line in thermometerCSVFile:
        if count == 0:
            # wifiWriter.writerow(["id","clientid","timestamp","sensor_id"])
            # wemoWriter.writerow(["id","currentmilliwatts","ontodayseconds","timestamp","sensor_id"])
            thermometerWriter.writerow(["id","temperature","timestamp","sensor_id"])
            count += 1
            continue

        line = line.strip()

        row = line.split(",")
        row[2] = datetime.fromtimestamp(int(row[2])/1000).strftime('%Y-%m-%d %H:%M:%S')
        thermometerWriter.writerow(row)

        if count % 100000 == 0:
            print ("Observation Modified ", count)
        count += 1

    count = 0
    for line in wemoCSVFile:
        if count == 0:
            # wifiWriter.writerow(["id","clientid","timestamp","sensor_id"])
            wemoWriter.writerow(["id","currentmilliwatts","ontodayseconds","timestamp","sensor_id"])
            # thermometerWriter.writerow(["id","temperature","timestamp","sensor_id"])
            count += 1
            continue

        line = line.strip()

        row = line.split(",")
        row[3] = datetime.fromtimestamp(int(row[3])/1000).strftime('%Y-%m-%d %H:%M:%S')
        wemoWriter.writerow(row)

        if count % 100000 == 0:
            print ("Observation Modified ", count)
        count += 1

    wifiCSVFile.close()
    wemoCSVFile.close()
    thermometerCSVFile.close()

    spark_wifiCSVFile.close()
    spark_wemoCSVFile.close()
    spark_thermometerCSVFile.close()


def copySemanticObservations(src, dest, mapping):
    if mapping == 1:
        return

    presenceCSVFile = open("{}presence.csv".format(dest))
    occupancyCSVFile = open("{}occupancy.csv".format(dest))

    spark_presenceCSVFile = open("{}spark_presence.csv".format(dest), "w")
    spark_occupancyCSVFile = open("{}spark_occupancy.csv".format(dest), "w")

    presenceWriter = csv.writer(spark_presenceCSVFile, quotechar="\"")
    occupancyWriter = csv.writer(spark_occupancyCSVFile, quotechar="\"")

    count = 0
    for line in presenceCSVFile:
        if count == 0:
            presenceWriter.writerow(["id","semantic_entity_id","location","timestamp","virtual_sensor_id"])
            # occupancyWriter.writerow(["id","semantic_entity_id","occupancy","timestamp","virtual_sensor_id"])
            count += 1
            continue

        line = line.strip()

        row = line.split(",")
        row[3] = datetime.fromtimestamp(int(row[3]) / 1000).strftime('%Y-%m-%d %H:%M:%S')
        presenceWriter.writerow(row)

        if count % 100000 == 0:
            print ("S Observation Modified ", count)
        count += 1

    count = 0
    for line in presenceCSVFile:
        if count == 0:
            # presenceWriter.writerow(["id", "semantic_entity_id", "location", "timestamp", "virtual_sensor_id"])
            occupancyWriter.writerow(["id","semantic_entity_id","occupancy","timestamp","virtual_sensor_id"])
            count += 1
            continue

        line = line.strip()

        row = line.split(",")
        row[3] = datetime.fromtimestamp(int(row[3]) / 1000).strftime('%Y-%m-%d %H:%M:%S')
        occupancyWriter.writerow(row)

        if count % 100000 == 0:
            print("S Observation Modified ", count)
        count += 1

    presenceCSVFile.close()
    occupancyCSVFile.close()

    spark_presenceCSVFile.close()
    spark_occupancyCSVFile.close()


if __name__ == "__main__":

    mapping = int(sys.argv[1])
    copyObservations("data/", "data/", mapping)
    copySemanticObservations("data/", "data/", mapping)
