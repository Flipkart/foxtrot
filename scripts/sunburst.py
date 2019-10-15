import pprint
import requests

url = "http://localhost:9200/consoles_v2/_search/?size=150"

r = requests.get(url)
data = r.json()
data = data["hits"]["hits"]

for hit in data:
    for section in hit["_source"]["sections"]:
        found = False
        for tile in section["tileData"]:
            chartType = section["tileData"][tile]["tileContext"]["chartType"]
            if chartType == "sunburst":
                pprint.pprint(hit["_source"]["name"])
                found = True
                break
        if found == True:
            found = False
            break
