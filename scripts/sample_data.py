import json
import random
import requests
import string
import time

HEADERS = {
    "Content-Type": "application/json"
}

field1 = ['a', 'b', 'c', 'd', 'e']
field2 = [1, 2, 3, 4, 5]


def create_table(table):
    request = {
        "name": table,
        "ttl": 30
    }
    requests.post(url="http://localhost:17000/foxtrot/v1/tables", headers=HEADERS, data=json.dumps(request))


def persist_documents(table, documents):
    requests.post(url="http://localhost:17000/foxtrot/v1/document/{}/bulk".format(table), headers=HEADERS,
                  data=json.dumps(documents))


def random_value(character_count):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(character_count))


def current_time():
    return int(time.time()) * 1000


def pick_random(data):
    return random.choice(data)


def create_documents():
    documents = []
    for _ in range(1, 500):
        documents.append({
            "id": random_value(20),
            "timestamp": current_time(),
            "data": {
                "key1": random_value(20),
                "key2": random_value(20),
                "key3": random_value(20),
                "key4": random_value(20),
                "low1": pick_random(field1),
                "low2": pick_random(field2)
            }
        })

    return documents


table = 'test'
create_table(table)
for i in range(1, 100):
    persist_documents(table, create_documents())
