import sys
import json

userTypes = []

def addUserType(tokens):
    usertypes_dict = {
        "name": tokens[0], 
        "probabilities": []
    }
    for i in range(1, 1+len(tokens[1:]), 2):
        prob = {
            "type": tokens[i],
            "prob": (tokens[i+1]),
        }
        usertypes_dict['probabilities'].append(prob)
    userTypes.append(eventtypes_dict)


def dumpJSON(outputPath):
    with open(outputPath, 'w') as writer:
        json.dump(userTypes, writer, indent=4)

def userTypesCSVToJSON(inputPath, outputPath):
    with open(inputPath, 'r') as csvFile:
        labels = csvFile.readline().split(',')
        for line in csvFile:
            addUserType(line.strip().split(','))
        dumpJSON(outputPath)

if __name__ == '__main__':
    userTypesCSVToJSON(sys.argv[1], sys.argv[2])
