import argparse
import pprint
import requests
import time

parser = argparse.ArgumentParser(
    description='Deleting unassigned indices beyond certain day')
parser.add_argument('--day', type=int, metavar='N', action='store',
                    help='Days beyond which indices needs to be deleted',
                    required=True)
parser.add_argument('--host', type=str, metavar='N', action='store',
                    help='Elasticsearch Host',
                    required=True)
args = parser.parse_args()

host = "http://"+args.host+":9200/"


r = requests.get(host + "_cat/shards/foxtrot-*-table*?format=JSON&h=index,shard,state,unassigned.reason")
data = r.json()

unassigned_indices = set()
for index in data:
    if (index["state"] == "UNASSIGNED"):
        unassigned_indices.add(index["index"])    

pprint.pprint("Unassigned Indices: ")
pprint.pprint("================================")
pprint.pprint(unassigned_indices)
pprint.pprint("================================")

oneDaySeconds = 86400
oneMonthSeconds = 2592000
currentTime = int(time.time())

for unassigned_index in unassigned_indices:
    res = requests.get(host + unassigned_index +"/_settings")
    index = res.json()
    creation_date = index[unassigned_index]["settings"]["index"]["creation_date"]
    if (currentTime * 1000 - int(
            creation_date)) > args.day * oneDaySeconds * 1000:
        pprint.pprint(host + unassigned_index)
        r = requests.delete(host + unassigned_index)
        pprint.pprint("Deleted index: "+unassigned_index)
        time.sleep(1)
