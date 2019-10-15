import argparse
import datetime
import json
import pprint
import requests
import time

host = "prd-esfoxtrot601.phonepe.nm1"


def convertToGb(dataSize):
    sizeUnit = dataSize[-2:].lower()
    value = float(dataSize[:-2])
    switcher = {
        "kb": value * 0.000001,
        "mb": value * 0.001,
        "gb": value,
        "tb": value * 1000
    }
    if sizeUnit in switcher:
        return switcher[sizeUnit]
    else:
        print ("Found unknown size unit: ")
        return 0


def getData():
    url = "http://" + host + ":9200/_cat/shards/"
    r = requests.get(url)
    data = r.text
    return data.splitlines()


def createMonitoringDict(dataSplitLines):
    monitorDict = {}
    now = datetime.datetime.now()
    date = "table-%d-%d-" % (now.day, now.month)
    for data in dataSplitLines:
        data = data.split()
        if len(data) < 8:
            continue
        if date not in data[0]:
            continue
        size = data[5]
        host = data[7]
        if host not in monitorDict:
            monitorDict[host] = {}
            monitorDict[host]["shardCount"] = 1
            monitorDict[host]["size"] = convertToGb(size)
        else:
            monitorDict[host]["shardCount"] += 1
            monitorDict[host]["size"] += convertToGb(size)
    return monitorDict


def getNodeNameMap():
    url = "http://" + host + ":9200/_nodes/stats"
    r = requests.get(url)
    data = json.loads(r.text)
    nodeNameDict = {}
    for node in data["nodes"]:
        nodeNameDict[data["nodes"][node]["name"]] = node
    return nodeNameDict


alreadyMovedShard = {}


def moveShard(dataSplitLines, fromHost, toHost, nodeMap):
    now = datetime.datetime.now()
    date = "table-%d-%d-" % (now.day, now.month)
    maxSize = 0
    maxSizeTable = ""
    maxSizeShard = 0
    for data in dataSplitLines:
        data = data.split()
        if len(data) < 8:
            continue
        if date not in data[0]:
            continue
        if fromHost != data[7]:
            continue
        if "STARTED" != data[3]:
            continue
        if (data[0] in alreadyMovedShard) and (
                alreadyMovedShard[data[0]] == data[1]):
            continue
        size = data[5]
        calSize = convertToGb(size)
        if calSize > maxSize:
            maxSize = calSize
            maxSizeTable = data[0]
            maxSizeShard = data[1]
    relocateShard(maxSizeTable, maxSizeShard, fromHost, toHost, nodeMap)


def relocateShard(index, shard, fromHost, toHost, nodeMap):
    body = {
        "commands": [
            {
                "move": {
                    "index": index, "shard": shard,
                    "from_node": nodeMap[fromHost], "to_node": nodeMap[toHost]
                }
            }
        ]
    }
    alreadyMovedShard[index] = shard
    json_string = json.dumps(body)
    print (body)
    url = "http://" + host + ":9200/_cluster/reroute"
    # print ("Starting reroute of index: " + index + " shard: " + shard + " from: " + fromHost + " to: " + toHost)
    try:
        r = requests.post(url=url, data=json_string)
        time.sleep(3)
        if 200 <= r.status_code <= 299:
            print (
                    "Reroute of index acknowledged: " + index + " shard: " + shard + " from: " + fromHost + " to: " + toHost)
    except requests.exceptions.RequestException as e:
        # print ("Reroute of index failed: " + index + " shard: " + shard + " from: " + fromHost + " to: " + toHost)
        print (e)


parser = argparse.ArgumentParser(description='Cluster Rerouting')
parser.add_argument('--shards', type=int, metavar='N', action='store',
                    help='Number of shards to be moved',
                    required=True)
parser.add_argument('--f', action='store', type=str,
                    help='Host from which the shard has to be moved',
                    required=True)
parser.add_argument('--t', action='store', type=str,
                    help='Host to which the shard has to be moved',
                    required=True)
args = parser.parse_args()

dataSplitLines = getData()
dic = createMonitoringDict(dataSplitLines)
pprint.pprint(dic)
nodeNameDict = getNodeNameMap()
for i in range(0, args.shards):
    moveShard(dataSplitLines, args.f, args.t, nodeNameDict)
