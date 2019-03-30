import json
common = ["infrastructureType", "infrastructure",
          "sensorType", "sensor", "user"]


def copyFiles(files, src, dest):
    for file in files:
        with open(src + file +".json", "r") as r:
            data = json.loads(r.read())

            if file == "infrastructureType":
                with open(dest + "spark_" + file + ".csv", "w") as w:
                    w.write(",".join(["ID", "DESCRIPTION", "NAME"]) + "\n")
                    for line in data:
                        w.write(",".join([line["id"], line["description"], line["name"]]) + "\n")

            if file == "infrastructure":
                with open(dest + "spark_" + file + ".csv", "w") as w:
                    w.write(",".join(["NAME", "INFRASTRUCTURE_TYPE_ID", "ID", "FLOOR"]) + "\n")
                    for line in data:
                        w.write(",".join([line["name"], line["type_"]["id"], line["id"], str(line["floor"])]) + "\n")

            if file == "sensorType":
                with open(dest + "spark_" + file + ".csv", "w") as w:
                    w.write(",".join(["ID", "MOBILITY", "DESCRIPTION", "NAME", "CAPTURE_FUNCTIONALITY", "PAYLOAD_SCHEMA"]) + "\n")
                    for line in data:
                        w.write(",".join([line["id"], "None", line["description"], line["name"], "None", "None"]) + "\n")

            if file == "sensor":
                with open(dest + "spark_" + file + ".csv", "w") as w:
                    w.write(",".join(["ID", "NAME", "INFRASTRUCTURE_ID", "USER_ID", "SENSOR_TYPE_ID", "SENSOR_CONFIG"]) + "\n")
                    for line in data:
                        w.write(",".join([line["id"], line["name"], line["infrastructure"]["id"], line["owner"]["id"], line["type_"]["id"], "None"]) + "\n")

                with open(dest + "spark_" + "coverage_infrastructure" + ".csv", "w") as w:
                    w.write(",".join(["SENSOR_ID", "INFRASTRUCTURE_ID"]) + "\n")
                    for line in data:
                        for room in line["coverage"]:
                            w.write(",".join([line["id"], room["id"]]) + "\n")

            if file == "user":
                with open(dest + "spark_" + file + ".csv", "w") as w:
                    w.write(",".join(["EMAIL", "GOOGLE_AUTH_TOKEN", "NAME", "ID"]) + "\n")
                    for line in data:
                        w.write(",".join([line["emailId"], line["googleAuthToken"], line["name"], line["id"]]) + "\n")



copyFiles(common, "/home/peeyush/Benchmark/benchmark/src/main/resources/data/", "data/")
