admFiles = ["location.adm", "infrastructureType.adm", "infrastructure.adm",
            "sensorType.adm", "group.adm", "platformType.adm", "sensor.adm", "platform.adm", "user.adm"]

collections = ["Location", "InfrastructureType", "Infrastructure",
               "SensorType", "Group", "PlatformType", "Sensor", "Platform", "User"]

TEMPLATE = """LOAD DATASET {} USING localfs(("path"="localhost://{}"),("format"="adm"));"""


