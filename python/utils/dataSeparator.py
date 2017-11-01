import shutil
import os

def countLines(file):
    count = 0
    with open(file) as fp:
        for line in fp:
            count += 1

    return count


def separateData(percentage, dataDir):
    totLines = countLines(dataDir + 'observation.json')

    numIngestionLines = int(((100-percentage)/100.0)*totLines)

    obsFile = open(dataDir + 'observation.json')
    tempFile = open(dataDir + 'observationTemp.json', 'w')
    insertTestFile = open(dataDir + 'insertTestData.json', 'w')

    lineNum = 1
    for line in obsFile:
        if lineNum == numIngestionLines:
            if line.strip() != ",":
                tempFile.write(line)
            if lineNum != totLines:
                tempFile.write("]")
                insertTestFile.write("[\n")
        elif lineNum < numIngestionLines:
            tempFile.write(line)
        else:
            if lineNum == 1:
                insertTestFile.write("[")
            elif lineNum == numIngestionLines +1 and line.strip() == ",":
                pass
            else:
                insertTestFile.write(line)
        lineNum += 1

    obsFile.close()
    tempFile.close()
    insertTestFile.close()

    shutil.copy2(dataDir + 'observationTemp.json', dataDir + 'observation.json')
    os.remove(dataDir + 'observationTemp.json')