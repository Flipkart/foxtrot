#!/usr/bin/env python
import argparse
import json
import random
import requests
import time
import uuid

# activityTemplate = [[ "Home"], ["Mini"]]
# activityTemplate = [[ "Home"], ["Mini", "Sedan", "Prime"], ["RideNow", "RideLater"], ["Confirm", "Cancel"]]
activityTemplate = [["Pay", "Request", "Split"],
                    ["PhonePicked", "VpaPicked", "Cancelled"],
                    ["Wallet", "Account"],
                    ["Finished", "Error"]]

categoryNames = ["Electronics", "Media", "Fashion"]
cityNames = ["Kolkata", "Bangalore", "Delhi", "Chennai", "Mumbai"]

eventNames = ['LOGIN', 'LOGIN', 'LOGIN', 'LOGIN', 'LOGIN', 'LOGIN', 'PAY',
              'PAY', 'PAY', 'PAY', 'PAY', 'REQUEST',
              'REQUEST', 'LOGOUT']
os = [['android', 'ics'], ['android', 'jellybean'], ['android', 'lollypop'],
      ['android', 'lollypop'],
      ['android', 'kitkat'], ['android', 'kitkat'], ['android', 'kitkat'],
      ['android', 'kitkat'], ['android', 'kitkat'],
      ['android', 'marshmallow'], ['android', 'marshmallow'],
      ['android', 'marshmallow'], ['ios', '8'], ['ios', '9'],
      ['ios', '9'], ['ios', '9']]


def createEvent():
    event = dict()
    event['id'] = str(uuid.uuid4())
    event['timestamp'] = long(time.time() * 1000)
    data = dict()
    data['eventType'] = random.choice(eventNames)
    osData = random.choice(os)
    data['os'] = osData[0]
    data['version'] = osData[1]
    data['latency'] = random.randint(7, 100)
    event['data'] = data
    return event


def eventBatch(batchSize=100):
    events = []
    for i in range(0, batchSize):
        events.append(createEvent())
        return events


def postBatch(args):
    events = eventBatch()
    r = requests.post(
        url="http://" + args.server + "/foxtrot/v1/document/test/bulk",
        data=json.dumps(events),
        headers={'Content-type': 'application/json'})
    print r
    if r.status_code == requests.codes.created:
        print "Sent batch"
    else:
        print "Error running query: " + str(r)
        time.sleep(1)


# def createEvent():
#    session = dict()
#    session['sessionId'] = str(uuid.uuid4())
#    session['sessionStartTime'] = long(time.time() * 1000)
#    sessionAttributes = dict()
#    sessionAttributes['category'] = random.choice(categoryNames)
#    sessionAttributes['time'] = session['sessionStartTime']
#    session['attributes'] = sessionAttributes
#    numSteps = random.randint(1,len(activityTemplate))
#    #numSteps = 4
#    city = random.choice(cityNames)
#    for i in range(0, numSteps):
#        activities = []
#        activity = dict()
#        activity['timestamp'] = long(time.time() * 1000)
#        activity['state'] = random.choice(activityTemplate[i])
#        activityAttributes = dict()
#        activityAttributes['city'] = city
#        activity['attributes'] = activityAttributes
#        activities.append(activity)
#        session['activities'] = activities
#        print json.dumps(session)
#        #print numSteps
#        r = requests.post(url="http://" + args.server + "/v1/activities/test", data=json.dumps(session), headers={'Content-type': 'application/json'})
#        if r.status_code == requests.codes.ok:
#            print "Saved"
#        else:
#            print "Error running query: " + str(r)
#
#
# def createDocument(argv):
#    opts,
#

parser = argparse.ArgumentParser(
    description='Send synthetic events to foxtrot for testing')
parser.add_argument('--count', type=int, metavar='N', action='store',
                    default=10,
                    help='the number of events to be sent (default: 10)')
parser.add_argument('--server', metavar='host:port', action='store',
                    help='foxtrot server host:port',
                    required=True)
args = parser.parse_args()

for i in range(0, args.count):
    postBatch(args)

    # for i in range(0,args.count):
    #    foxtrotEvent=dict()
    #    foxtrotEvent['id'] = str(uuid.uuid4())
    #    foxtrotEvent['timestamp'] = long(time.time() * 1000)
    #    data=dict()
    #    data['eventType'] = random.choice(eventTypes)
    #    foxtrotEvent['data']=data
    #    r = requests.post(url="http://" + args.server + "/foxtrot/v1/document/testapp", data=json.dumps(foxtrotEvent), headers={'Content-type': 'application/json'})
    #    if r.status_code == requests.codes.created:
    #        print "Save"
    #    else:
    #        print "Error running query: " + str(r)
    #
