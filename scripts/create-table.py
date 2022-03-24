import argparse
import json
import requests

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", required=True, help="Table name to Create")
    parser.add_argument("--ttl", required=True, help="TTL for table")
    parser.add_argument("--host", required=True, help="Foxtrot Host")
    parser.add_argument("--port", required=True, help="Foxtrot Port")
    args = vars(parser.parse_args())

    url = "http://" + args["host"] + ":" + args["port"] + "/foxtrot/v1/tables"
    table = {"name": args["name"], "ttl": args["ttl"]}
    data = table
    headers = {'Content-Type': 'application/json'}
    print(data)
    request = requests.post(url, json=data, headers=headers)
    print(request.text)
