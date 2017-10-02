
import datetime
import random
import json

dt = datetime.datetime(2017, 07, 11, 0, 0, 0)
end = datetime.datetime(2017, 07, 11, 23, 59, 59)
step = datetime.timedelta(seconds=600)

infra = ['2011', '2013', '3224', '5011', '1100', '1200', '3081', '6218']
obsList = []

while dt < end:

	for i in range(8):
		obs = {
			"timestamp": dt.strftime('%Y-%m-%d %H:%M:%S'),
			"virtualSensorId": "vSensor2",
			"payload": {
				"occupancy": random.randint(0, 100)
			},
			"typeId": "occupancy",
			"semanticEntityId": infra[i]
		}
		obsList.append(obs)
    	dt += step

with open('occupancySO.json', 'w') as fp:
    json.dump(obsList, fp)


