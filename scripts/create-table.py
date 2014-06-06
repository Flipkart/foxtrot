import urllib2
import json
import argparse


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", required=True, help="Table name to Create")
    parser.add_argument("--ttl", required=True, help="TTL for table")
    parser.add_argument("--h", required=True, help="Foxtrot Host")
    parser.add_argument("--p", required=True, help="Foxtrot Port")
    args = vars(parser.parse_args())

    url = "http://" + args["h"] + ":" + args["p"]  + "/foxtrot/v1/tables"
    table = { "name" : args["name"], "ttl" : args["ttl"] }
    data = json.dumps(table)
    headers = {'Content-Type': 'application/json'}
    request = urllib2.Request(url, data, headers)
    response = urllib2.urlopen(request)
    response.close()
