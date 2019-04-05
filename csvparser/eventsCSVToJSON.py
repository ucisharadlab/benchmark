import sys
import json

events = []
leisureEventDuration = 15 # in minutes

def addEvent(labels, tokens):
    event_dict = {
        "name": tokens[0], 
        "id": int(tokens[1]),
        "type": tokens[2],
        "spaces": tokens[3].split(';'),
        "startTime": tokens[4] + ":00",
        "endTime": tokens[5] + ":00",
        "isPeriodic": "True" if tokens[6] == "True" else "False",
        "periodDays": tokens[7] if tokens[6] == "True" else "",
        "capacity": []
    }
    for i in range(8, 8+len(tokens[8:]), 3):
        cap = {
            "type": tokens[i],
            "lo": (tokens[i+1]),
            "hi": (tokens[i+2])
        }
        event_dict['capacity'].append(cap)
    events.append(event_dict)


def dumpJSON(outputPath):
    with open(outputPath, 'w') as writer:
        json.dump(events, writer, indent=4)

def eventsCSVToJSON(inputPath, outputPath):
    with open(inputPath, 'r') as csvFile:
        labels = csvFile.readline().split(',')
        for line in csvFile:
            addEvent(labels, line.strip().split(','))
        dumpJSON(outputPath)

if __name__ == '__main__':
    eventsCSVToJSON(sys.argv[1], sys.argv[2])
