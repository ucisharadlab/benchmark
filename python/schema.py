import requests
import urllib.parse


SERVER = "http://localhost:19001"
SCHEMA_FILE = "schema.sqlpp"


def createSchema():

    ddlQueries = open(SCHEMA_FILE).read()
    data = {
        "query": ddlQueries,
        "query-language": "SQLPP",
        "output-format":"CLEAN_JSON",
        "execute-query":"true"
    }
    response = requests.post(SERVER, data)

    if response.status_code == 200:
        print("Schema Successfully Created")
    else:
        print("Schema Creation Failed")

createSchema()