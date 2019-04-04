import json

in_dir = '/home/joyce/benchmark/src/main/resources/data/'
out_dir = '/home/joyce/benchmark/python/convert/'

def match_sensor_type(type):
  
  if type == "WiFiAP":
    return "BO_WiFiAP"

  if type == "WeMo":
    return "BO_WeMo"

  if type == "Thermometer":
    return "BO_Thermometer"
  
  if type == "Beacon":
    return "BO_Beacon"

  if type == "EnergyMeter":
    return "BO_EnergyMeter"

  if type == "Camera":
    return "BO_Camera"

sensors_output = []

with open(in_dir + "sensor.json", "r") as read_file:
  sensors = json.loads(read_file.read())

  for sensor in sensors:

    sensor_output = {}

    if sensor['name']:
      sensor_output["label"] = sensor['name']

    if sensor['infrastructure']:
      location = sensor['infrastructure']['geometry'][0]
      sensor_output["location"] = location

    if sensor['type_']:
      sensor_type = match_sensor_type(sensor['type_']['id'])
      sensor_output["type"] = sensor_type

    if sensor['coverage']:
      spaces = []
      coverage = {}

      for coverage in sensor['coverage']:
        space_id = 'DBH_' + coverage['id']
        spaces.append(space_id)

      coverage["id"] = space_id + "_Coverage"
      coverage["space"] = spaces
      sensor_output["coverage"] = coverage

    sensors_output.append(sensor_output)

with open(out_dir + 'sensor.json', 'w') as outfile:
  json.dump(sensors_output, outfile)

""" 
input
{
  "name": "1100", 
  "floor": 1, 
  "geoobject": [
      {
          "y": 0, 
          "x": 34, 
          "z": 0, 
          "id": "ccc524e9-bc94-436b-9400-b92395bb14e1"
      }, 
      {
          "y": 20, 
          "x": 63, 
          "z": 0, 
          "id": "d57240a7-15c9-4b83-bcbb-16f06f416003"
      }
  ], 
  "type_": {
      "id": "class_room", 
      "description": "Class Room Description", 
      "name": "Class Rooms"
  }, 
  "id": "1100"
}
output
{
  "id": "3142-clwa-2065",
  "label": "DBH WiFi Access Point in 2065",
  "type": "BO_WiFiAP",
  "location": {
    "id": "DBH_AP2065_Location",
    "x": 0,
    "y": 0,
    "z": 0
  },
  "description": "WiFi Access Point",
  "coverage": {
    "id": "DBH_AP2065_Coverage",
    "space": [
      "DBH_2065",
      "DBH_2099"
    ],
    "radius": 5
  },
  "mobility": "Fixed",
  "transformer_code_language": "Java",
  "transformer_project_name": "wifitopresence"
}
"""
"""
input:
{
  "name": "1100",
  "floor": 1,
  "geoobject": [
    {
      "y": 0,
      "x": 34,
      "z": 0,
      "id": "ccc524e9-bc94-436b-9400-b92395bb14e1"
    },
    {
      "y": 20,
      "x": 63,
      "z": 0,
      "id": "d57240a7-15c9-4b83-bcbb-16f06f416003"
    }
  ],
  "type_": {
    "id": "class_room",
    "description": "Class Room Description",
    "name": "Class Rooms"
  },
  "id": "1100"
}
output:
[
    {
        "id": "BO_DBH",
        "label": "Donald Bren Hall",
        "type": "BO_Building",
        "description": "The Donald Bren School of Information and Computer Sciences (ICS) is the first and only computer science school in the University of California.",
        "geoobject": {
            "id": "DBH_GEOOBJECT",
            "coordinates": [
                {
                    "id": "DBH_GEOOBJECT_Coordinate1",
                    "label": "DBH_Coordinate1",
                    "x": 0,
                    "y": 0,
                    "z": 1
                },
                {
                    "id": "DBH_GEOOBJECT_Coordinate2",
                    "label": "DBH_Coordinate2",
                    "x": 397,
                    "y": -121,
                    "z": 6
                }
            ]
        }
    },
    {
        "id": "DBH_Floor_3",
        "label": "3rd floor in Donald Bren Hall",
        "type": "BO_Floor",
        "description": "Third floor accommodates faculty offices, research labs, classrooms, and meeting rooms.",
        "geoobject": {
            "id": "DBH_Floor_3_GEOOBJECT",
            "coordinates": [
                {
                    "id": "DBH_Floor_3_GEOOBJECT_Coordinate1",
                    "label": "DBH_Coordinate1",
                    "x": 0,
                    "y": 0,
                    "z": 2
                },
                {
                    "id": "DBH_Floor_3_GEOOBJECT_Coordinate2",
                    "label": "DBH_Coordinate2",
                    "x": 397,
                    "y": -121,
                    "z": 2
                }
            ]
        }
    },
    {
        "id": "DBH_3011",
        "label": "3011",
        "type": "BO_Conference_Room",
        "description": "Conference room provided for ISG group",
        "properties": [
            {
                "id": "BO_Conference_Room_Occupancy",
                "observedby": "3143-clwa-3019"
            }
        ],
        "geoobject": {
            "id": "DBH_3011_GEOOBJECT",
            "coordinates": [
                {
                    "id": "DBH_3011_GEOOBJECT_Coordinate1",
                    "label": "3011_Coordinate1",
                    "x": 25,
                    "y": 100,
                    "z": 2
                },
                {
                    "id": "DBH_3011_GEOOBJECT_Coordinate2",
                    "label": "3011_Coordinate2",
                    "x": 75,
                    "y": 150,
                    "z": 2
                },
                {
                    "id": "DBH_3011_GEOOBJECT_Coordinate3",
                    "label": "3011_Coordinate3",
                    "x": 25,
                    "y": 150,
                    "z": 2
                },
                {
                    "id": "DBH_3011_GEOOBJECT_Coordinate4",
                    "label": "3011_Coordinate4",
                    "x": 75,
                    "y": 100,
                    "z": 2
                }
            ]
        }
    }
]
"""
