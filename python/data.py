import requests


SERVER = "http://localhost:19001"
ABS_PATH = "/home/peeyush/Asterix/benchmark/"
DATADIR = "data/TQL_OLD/"

COLLECTIONS = {
    "Location", "Region", "InfrastructureType", "Infrastructure", "User", "UserGroup",
    "ObservationType", "SensorType", "Sensor", "PlatformType", "Platform"
}


def postData(collection):
    with open(DATADIR + collection + ".json") as r:
        values = r.read()

    data = {
        "query": "Use Tippers; INSERT INTO {}({})".format(collection, values),
        "query-language": "SQLPP",
        "output-format":"CLEAN_JSON",
        "execute-query":"true"
    }

    response = requests.post(SERVER, data)
    print (response.text)
    if response.status_code == 200:
        print("{} Successfully Added".format(collection))
    else:
        print("{} Addition Failed".format(collection))


def postComplete():
    for collection in COLLECTIONS:
        postData(collection)

def postObservations():
    postData("Observation")

def postObservationsADM():
    collection = "Observation"
    data = {
        "query": ('Use Tippers; LOAD DATASET {} USING localfs '
                  '(("path"="{}://{}"),("format"="adm"))'.format(
            collection, "localhost", ABS_PATH+DATADIR+collection+".json")),
        "query-language": "SQLPP",
        "output-format":"CLEAN_JSON",
        "execute-query":"true"
    }

    response = requests.post(SERVER, data)
    print (response.text)
    if response.status_code == 200:
        print("{} Successfully Added".format(collection))
    else:
        print("{} Addition Failed".format(collection))


#postObservationsADM()
postComplete()