import random
import json
import uuid


def createUsers(numUsers, src, dest):

    with open(src+'user.json') as data_file:
        users = json.load(data_file)

    with open(src+'group.json') as data_file:
        groups = json.load(data_file)

    numGroups = len(groups)

    for i in range(numUsers):
        id = str(uuid.uuid1()),
        groupList = [groups[random.randint(0, numGroups-1)]]
        user = {
            "id": id[0],
            "googleAuthToken": id[0],
            "name": "simUser{}".format(i),
            "emailId": "simUser{}@uci.edu".format(i) ,
            "groups": groupList
        }
        users.append(user)

    with open(dest + 'user.json', 'w') as writer:
        json.dump(users, writer, indent=4)
