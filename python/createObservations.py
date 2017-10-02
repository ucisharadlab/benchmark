import datetime
import random
import json

dt = datetime.datetime(2017, 7, 11, 0, 0, 0)
end = datetime.datetime(2017, 9, 11, 23, 59, 59)
step = datetime.timedelta(seconds=500)

ROWS = "data/rows/"
OBJECTS = "data/objects/"
MIX = "data/mix/"

obsRowList = []
obsObjectList = []
obsMixList = []

# with open(ROWS + "wemoData.json") as data_file:
#     wemoData = json.load(data_file)
#
# with open(ROWS + "wifiAPdata.json") as data_file:
#     wifiAPData = json.load(data_file)
#
# with open(ROWS + "temperatureData.json") as data_file:
#     temperatureData = json.load(data_file)
#
# obsRowList = wemoData + wifiAPData + temperatureData
#
# with open(ROWS + 'observation.json', 'w') as fp:
#     json.dump(obsRowList, fp)


with open(OBJECTS + "wemoData.json") as data_file:
    wemoData = json.load(data_file)

with open(OBJECTS + "wifiAPdata.json") as data_file:
    wifiAPData = json.load(data_file)

with open(OBJECTS + "temperatureData.json") as data_file:
    temperatureData = json.load(data_file)

obsObjectList = wemoData + wifiAPData + temperatureData

with open(OBJECTS + 'observation.json', 'w') as fp:
    json.dump(obsObjectList, fp)