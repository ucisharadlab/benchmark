import sys
import json

eventTypes = []

def addEventType(tokens):
    eventtypes_dict = {
        "name": tokens[0], 
        "probabilities": []
    }
    for i in range(1, 1+len(tokens[1:]), 2):
        prob = {
            "type": tokens[i],
            "prob": (tokens[i+1]),
        }
        eventtypes_dict['probabilities'].append(prob)
    eventTypes.append(eventtypes_dict)


def dumpJSON(outputPath):
    with open(outputPath, 'w') as writer:
        json.dump(eventTypes, writer, indent=4)

def eventTypesCSVToJSON(inputPath, outputPath):
    with open(inputPath, 'r') as csvFile:
        labels = csvFile.readline().split(',')
        for line in csvFile:
            addEventType(line.strip().split(','))
        dumpJSON(outputPath)

if __name__ == '__main__':
    eventTypesCSVToJSON(sys.argv[1], sys.argv[2])
