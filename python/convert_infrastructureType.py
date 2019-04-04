import json

in_dir = '/home/joyce/benchmark/src/main/resources/data/'
out_dir = '/home/joyce/benchmark/python/convert/'

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
  

infraTypes_output = []

with open(in_dir + "infrastructureType.json", "r") as f:
  infrastructureTypes = json.loads(f.read())

  for infrastructureType in infrastructureTypes:

    infraType_output = {}

    if infrastructureType['id']:
      infraType_output["id"] = match_space_type(infrastructureType['id'])

    if infrastructureType['name']:
      infraType_output["label"] = infrastructureType['name']

    if infrastructureType['description']:
      infraType_output["description"] = infrastructureType['description']

    infraType_output["subtype_of"] = "TIP_Space"

    infraTypes_output.append(infraType_output)

with open(out_dir + 'spaceType.json', 'w') as outfile:
  json.dump(infraTypes_output, outfile)