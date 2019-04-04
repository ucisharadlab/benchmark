import random
import json
import uuid


def createRooms(numRooms, src, dest):

    with open(src+'space.json') as data_file:
        rooms = json.load(data_file)

    print ("Creating Rooms")

    newRooms = []

    for i in range(numRooms-len(rooms)):
        id = str(uuid.uuid4())
        copiedRoom = rooms[random.randint(0, len(rooms) - 1)]
        room = {
            "id": id.replace('-', '_'),
            "label": "simRoom{}".format(i),
            "type": copiedRoom['type'],
            "description": copiedRoom['description'],
            #"floor": copiedRoom['floor'],
            "geoobject": copiedRoom['geoobject'],           
        }
        newRooms.append(room)

    newRooms.extend(rooms)

    with open(dest + 'space.json', 'w') as writer:
        json.dump(newRooms, writer, indent=4)
