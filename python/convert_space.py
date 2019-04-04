import json

in_dir = '/home/joyce/benchmark/src/main/resources/data/'
out_dir = '/home/joyce/benchmark/python/convert/'

spaces_output = []
space_count = 0

def match_space_type(type):
  
  if type == "class_room":
    return "BO_Classroom"

  if type == "office":
    return "BO_Office"

  if type == "utilities":
    return "BO_Utility_Room"
  
  if type == "corridor":
    return "BO_Corridor"

  if type == "seminar_room":
    return "BO_Seminar_Room"

  if type == "labs":
    return "BO_Labs"

  if type == "reception":
    return "BO_Reception"
  
  if type == "mail_room":
    return "BO_Mail_Room"

  if type == "work_room":
    return "BO_Work_Room"
  
  if type == "conference_room":
    return "BO_Conference_Room"
  
  if type == "meeting_room":
    return "BO_Meeting_Room"
  
  if type == "male_restroom":
    return "BO_Male_Restroom"

  if type == "female_restroom":
    return "BO_Female_Restroom"
  
  if type == "facility_room":
    return "BO_Facility_Room"

  if type == "elevator":
    return "BO_Elevator"

  if type == "kitchen":
    return "BO_Kitchen"
  

with open(in_dir + "infrastructure.json", "r") as read_infra:
  infrastructures = json.loads(read_infra.read())

  for infrastructure in infrastructures:

    space_output = {}
    floor = 0

    if infrastructure['name']:
      space_output["label"] = infrastructure['name']

    if infrastructure['floor']:
      floor = infrastructure['floor']

    if infrastructure['geometry']:
      geometries = infrastructure['geometry']
      geoobject = {}
      
      coordinates = []
      for geometry in geometries:
        geometry["z"] = floor
        coordinates.append(geometry)
      
      geoobject["coordinates"] = coordinates
      geoobject["id"] = "geo" + str(space_count)
      
      space_output["geo_object"] = geoobject

    if infrastructure['type_']:
      space_type = match_space_type(infrastructure['type_']['id'])
      space_output["type"] = space_type
    """
    properties = []
    property = {}
    property.put("id", type)
    property.put("observed_by", sensor)
    properties.put("properies", property)

    space_output["properties"] = properties
    """
    spaces_output.append(space_output)
    space_count += space_count+1

with open(out_dir + 'space.json', 'w') as outfile:
  json.dump(spaces_output, outfile)

