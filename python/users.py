import random
import json
import uuid


def createUsers(numUsers, src, dest):

    with open(src+'user.json') as data_file:
        users = json.load(data_file)

    with open(src+'group.json') as data_file:
        groups = json.load(data_file)

    with open(src+'platform.json') as data_file:
        platforms = json.load(data_file)

    with open(src+'platformType.json') as data_file:
        platformsTypes = json.load(data_file)

    numGroups = len(groups)

    for i in range(numUsers):
        id = str(uuid.uuid1())
        groupList = [groups[random.randint(0, numGroups-1)]]
        user = {
            "id": id,
            "googleAuthToken": id,
            "name": "simUser{}".format(i),
            "emailId": "simUser{}@uci.edu".format(i) ,
            "groups": groupList
        }
        users.append(user)

        id = str(uuid.uuid1())
        platform = {
            "id": id,
            "name": "simPlatform{}".format(i),
            "owner": user,
            "type_": platformsTypes[random.randint(1, len(platformsTypes) - 1)],
            "hashedMac": id
        }
        platforms.append(platform)

    with open(dest + 'user.json', 'w') as writer:
        json.dump(users, writer, indent=4)

    with open(dest + 'platform.json', 'w') as writer:
        json.dump(platforms, writer, indent=4)
