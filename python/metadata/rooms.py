import random
import json
import uuid


def createRooms(numRooms, src, dest):

    with open(src+'infrastructure.json') as data_file:
        rooms = json.load(data_file)

    print ("Creating Rooms")

    newRooms = []

    for i in range(numRooms-len(rooms)):
        id = str(uuid.uuid4())
        copiedRoom = rooms[random.randint(0, len(rooms) - 1)]
        room = {
            "id": id.replace('-', '_'),
            "name": "simRoom{}".format(i),
            "floor": copiedRoom['floor'],
            "geometry": copiedRoom['geometry'],
            "type_": copiedRoom['type_'],
        }
        newRooms.append(room)

    newRooms.extend(rooms)

    with open(dest + 'infrastructure.json', 'w') as writer:
        json.dump(newRooms, writer, indent=4)
