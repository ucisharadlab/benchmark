import json
import os

in_dir = '/home/joyce/benchmark/src/main/resources/data/'
out_dir = '/home/joyce/benchmark/python/convert/'

def match_user_type(type):
  
  if type == "admin":
    return "TIP_Admin"

  if type == "ISG":
    return "UO_ISG"

users_output = []

with open(in_dir + "user.json", "r") as f:
  users = json.loads(f.read())

for user in users:

    user_output = {}
    user_types = []

    if user['name']:
        user_output["name"] = user['name']

    if user['emailId']:
        user_output["email"] = user['emailId']

    if user['groups']:
        for group in user['groups']:
            user_type = match_user_type(group['id'])
            user_types.append(user_type)
        user_output["type"] = user_types

    users_output.append(user_output)

with open(out_dir + 'user.json', 'w') as outfile:
    json.dump(users_output, outfile)
