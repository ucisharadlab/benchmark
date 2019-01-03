import json


class Parser:

    def __init__(self, seedFile):
        self.data = json.load(open(seedFile))
        self.count = -1

    def getNext(self):
        try:
            self.count += 1
            return self.data[self.count]
        except Exception as e:
            return None


class ParserLine:

    def __init__(self, seedFile):
        self.fp = open(seedFile)

    def getNext(self):
        try:
            return json.loads(self.fp.readline())
        except Exception as e:
            return None

