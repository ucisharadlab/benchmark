import datetime
import random
import json
import wifiap, wemo, temperature

dt = datetime.datetime(2017, 11, 7, 0, 0, 0)
end = datetime.datetime(2017, 11, 13, 23, 59, 59)
step = datetime.timedelta(seconds=500)

ROWS = "data/rows/"
OBJECTS = "data/objects/"
line = None

wifiap.createWiFiObservations(dt, end, step)
wemo.createWemoObservations(dt, end, step)
temperature.createTemperaturebservations(dt, end, step)

finalRow = open(ROWS + 'observation.json', 'w')
finalRow .write("[\n")

wemoRow = open(ROWS + "wemoData.json")
for line in wemoRow:
    finalRow.write(line + ",\n")

wifiRow = open(ROWS + "wifiAPdata.json")
for line in wifiRow:
    finalRow.write(line + ",\n")

temperatureRow =  open(ROWS + "temperatureData.json")
for line in temperatureRow:
    finalRow.write(line + ",\n")

finalRow.write(line)

finalRow.write("\n]")

finalRow.close()
wifiRow.close()
temperatureRow.close()
wemoRow.close()


finalObj = open(OBJECTS + 'observation.json', 'w')
finalObj .write("[\n")

wemoObj = open(OBJECTS + "wemoData.json")
for line in wemoObj:
    finalObj.write(line + ",\n")

wifiObj = open(OBJECTS + "wifiAPdata.json")
for line in wifiObj:
    finalObj.write(line + ",\n")

temperatureObj = open(OBJECTS + "temperatureData.json")
for line in temperatureObj:
    finalObj.write(line + ",\n")

finalObj.write(line)

finalObj.write("\n]")

finalObj.close()
wifiObj.close()
temperatureObj.close()
wemoObj.close()
