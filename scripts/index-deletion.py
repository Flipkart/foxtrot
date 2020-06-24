import argparse
import pprint
import requests
import time

host = "http://localhost:9200/"

r = requests.get(host + "foxtrot-*-table*/_settings/index.creation_date")
data = r.json()
pprint.pprint(data)

oneDaySeconds = 86400
oneMonthSeconds = 2592000
currentTime = int(time.time())

parser = argparse.ArgumentParser(
    description='Deleting foxtrot data beyond certain day')
parser.add_argument('--day', type=int, metavar='N', action='store',
                    help='Days beyond which data needs to be deleted',
                    required=True)
args = parser.parse_args()

for indexName in data:
    creation_date = data[indexName]["settings"]["index"]["creation_date"]
    if (currentTime * 1000 - int(
            creation_date)) > args.day * oneDaySeconds * 1000:
        pprint.pprint(host + indexName)
        r = requests.delete(host + indexName)
        time.sleep(1)
