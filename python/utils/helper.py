
def deleteSensorAttributes(sensor):
    try:
        del sensor['infrastructure']
        del sensor['owner']
        del sensor['coverage']
        del sensor['sensorConfig']
        del sensor['type_']['description']
        del sensor['type_']['mobility']
        del sensor['type_']['captureFunctionality']
        del sensor['type_']['payloadSchema']

    except:
        pass
    return sensor


def deleteSOTypeAttributes(type_):
    try:
        del type_['description']
        del type_['payloadSchema']
    except:
        pass
    return type_

def deleteVirtualSensorAttributes(sensor):
    try:
        del sensor['description']
        del sensor['language']
        del sensor['type_']
        del sensor['projectName']
    except:
        pass
    return sensor

def deleteUserAttributes(user):
    try:
        del user['groups']
        del user['googleAuthToken']
    except:
        pass
    return user


def deleteInfraAttributes(infra):
    try:
        del infra['geometry']
        del infra['type_']['description']
    except:
        pass
    return infra
