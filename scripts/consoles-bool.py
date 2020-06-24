# Script to correct boolean value of consoles in es6
import json
import requests
import time

host = "localhost"
r = requests.get("http://" + host + ":9200/consoles_v2/_search/?size=1000")
# r = requests.get("http://localhost:9200/consoles_v2/_search/?size=1000")
data = r.json()
data = data["hits"]["hits"]
consolesData = {}

for hit in data:
    for section in hit["_source"]["sections"]:
        for tile in section["tileData"]:
            tableName = section["tileData"][tile]["tileContext"]["table"]
            if tableName not in consolesData:
                consolesData[tableName] = []
            for filt in section["tileData"][tile]["tileContext"]["filters"]:
                if "field" in filt:
                    if filt["field"] not in consolesData[tableName]:
                        consolesData[tableName].append(filt["field"])

# pprint.pprint(consolesData)

time.sleep(0.2)

mappingsData = {}


def getMappingType(data, fieldValue, tableName):
    for field in data:
        if fieldValue == "":
            fieldValueNow = field
        else:
            fieldValueNow = fieldValue + "." + field
        if "type" in data[field]:
            if data[field]["type"] == "boolean":
                if tableName not in mappingsData:
                    mappingsData[tableName] = []
                if fieldValueNow not in mappingsData[tableName]:
                    mappingsData[tableName].append(fieldValueNow)
                continue
        else:
            getMappingType(data[field]["properties"], fieldValueNow, tableName)


URL = "http://" + host + ":9200/*"

for tableName in consolesData:
    mappingsURL = URL + tableName + "*/_mappings"
    r = requests.get(mappingsURL)
    data = r.json()
    if "error" in data:
        print "Error in getting table: " + tableName
        continue
    for value in data.itervalues():
        if ("mappings" in value) and ("document" in value["mappings"]) and (
                "properties" in value["mappings"]["document"]):
            data = value["mappings"]["document"]["properties"]
        else:
            print "Error in getting mapping properties: " + tableName
            continue
        fieldValue = ""
        getMappingType(data, fieldValue, tableName)
    time.sleep(0.2)

# pprint.pprint(mappingsData)

time.sleep(0.2)

r = requests.get("http://" + host + ":9200/consoles_v2/_search/?size=1000")
# r = requests.get("http://localhost:9200/consoles_v2/_search/?size=1000")
data = r.json()
data = data["hits"]["hits"]
consolesData = {}

t = ["true", "TRUE", "YES", 1, "1"]
f = ["false", "FALSE", "NO", 0, "0"]
for hit in data:
    for i in range(0, len(hit["_source"]["sections"])):
        # for section in hit["_source"]["sections"]:
        for tile in hit["_source"]["sections"][i]["tileData"]:
            tableName = hit["_source"]["sections"][i]["tileData"][tile]["tileContext"]["table"]
            if tableName not in consolesData:
                consolesData[tableName] = []
            for j in range(0, len(hit["_source"]["sections"][i]["tileData"][tile]["tileContext"]["filters"])):
                # for filt in hit["_source"]["sections"][i]["tileData"][tile]["tileContext"]["filters"]:
                if "field" in hit["_source"]["sections"][i]["tileData"][tile]["tileContext"]["filters"][j]:
                    if (tableName in mappingsData):
                        if (hit["_source"]["sections"][i]["tileData"][tile]["tileContext"]["filters"][j]["field"] in
                                mappingsData[tableName]):
                            if hit["_source"]["sections"][i]["tileData"][tile]["tileContext"]["filters"][j][
                                "value"] in t:
                                hit["_source"]["sections"][i]["tileData"][tile]["tileContext"]["filters"][j][
                                    "value"] = "true"
                            elif hit["_source"]["sections"][i]["tileData"][tile]["tileContext"]["filters"][j][
                                "value"] in f:
                                hit["_source"]["sections"][i]["tileData"][tile]["tileContext"]["filters"][j][
                                    "value"] = "false"
                            print "Table Name: " + tableName + " Field Name: " + \
                                  hit["_source"]["sections"][i]["tileData"][tile]["tileContext"]["filters"][j][
                                      "field"] + "Value: " + \
                                  hit["_source"]["sections"][i]["tileData"][tile]["tileContext"]["filters"][j]["value"]
                    if filt["field"] not in consolesData[tableName]:
                        consolesData[tableName].append(
                            hit["_source"]["sections"][i]["tileData"][tile]["tileContext"]["filters"][j]["field"])
    URL = "http://" + host + ":9200/consoles_v2/"
    URL = URL + hit["_type"] + "/" + hit["_id"]
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    json_string = json.dumps(hit["_source"])
    r = requests.put(URL, json_string, headers=headers)
    time.sleep(0.2)
    print r.status_code
    # if r.status_code == 400:
    #     pprint.pprint(hit["_id"])
    #     break
