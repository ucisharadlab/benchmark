import json
from datetime import datetime

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

def copyObservations(src, dest):
    with open(src + "observation.json", "r") as r:
        observations = json.loads(r.read())
        strings = []
        for observation in observations:
            observation["timeStamp"] = \
                "datetime('" + datetime.strptime(observation["timeStamp"], "%Y-%m-%d %H:%M:%S") \
                    .strftime("%Y-%m-%dT%H:%M:%SZ") + "')"

            strings.append(str(observation)
                           .replace('"' + observation["timeStamp"] + '"', observation["timeStamp"])
                           .replace('"', '\\"')
                           .replace("'", '"')
                           )

    with open(dest + "observation.adm", "w") as w:
        w.write("{}".format('\n'.join(strings)))

if __name__ == "__main__":
    copyFiles(common, "../data/", "../adm/")
    copyObservations("../data/", "../adm/")

