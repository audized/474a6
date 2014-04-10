#  Storage node for Assignment 6, CMPT 474, Spring 2014

# Core libraries
import os
import sys
import time
import math
import json

# Libraries that have to have been installed by pip
import redis
import requests
import mimeparse
from bottle import route, run, request, response, abort

# Local libraries
from queueservice import Queue
from vectorclock import VectorClock

base_DB_port = 3000

# These values are defaults for when you start this server from the command line
# They are overridden when you run it from test/run.py
config = { 'id': 0,
           'servers': [{ 'host': 'localhost', 'port': 6379 }],
           'hostport': base_DB_port,
           'qport': 6000,
           'ndb': 1,
           'digest-length': 1}

if (len(sys.argv) > 1):
    config = json.loads(sys.argv[1])

# Gossip globals
qport = config['qport']
queue = Queue(qport)
id = config['id']
digest_list = []
db_id = 'db'+str(id)
ndb = config['ndb']
numWrites = 0

# Connect to a single Redis instance
client = redis.StrictRedis(host=config['servers'][0]['host'], port=config['servers'][0]['port'], db=0)

# A user updating their rating of something which can be accessed as:
# curl -XPUT -H'Content-type: application/json' -d'{ "rating": 5, "choices": [3, 4], "clocks": [{ "c1" : 5, "c2" : 3 }] }' http://localhost:3000/rating/bob
# Response is a JSON object specifying the new average rating for the entity:
# { rating: 5 }
@route('/rating/<entity>', method='PUT')
def put_rating(entity):
    # Check to make sure JSON is ok
    mimetype = mimeparse.best_match(['application/json'], request.headers.get('Accept'))
    if not mimetype: return abort(406)

    # Check to make sure the data we're getting is JSON
    if request.headers.get('Content-Type') != 'application/json': return abort(415)

    response.headers.append('Content-Type', mimetype)

    # Parse the request
    data = json.load(request.body)
    setrating = data.get('rating')
    setclock = VectorClock.fromDict(data.get('clock'))

    key = '/rating/'+entity

    # YOUR CODE HERE
    # MERGE WITH CURRENT VALUES FOR THIS KEY
    # REPLACE FOLLOWING WITH CORRECT FINAL RATING
    old_rating = client.hget(key, 'rating')
    finalrating = old_rating

    # if rating does not exist, add it. Otherwise..
    # SET THE RATING, CHOICES, AND CLOCKS IN THE DATABASE FOR THIS KEY
    # COMPUTE THE MEAN, finalrating
    writeToDB = True
    new_choices = []
    new_vcl = []
    writeToDB, finalrating, new_choices, new_vcl = merge(key, old_rating, setrating, setclock)

    # Add to digest list only if the PUT request triggers an update to the DB
    if writeToDB:
        global digest_list
        digest_list.append((db_id, key, finalrating, new_choices, new_vcl))
        global numWrites
        numWrites+=1

    # GOSSIP
    while 1:
        msg = queue.get(db_id)
        # Stop if there's no message in the queue
        if msg == None:
            break
        # Merge and pass on if the message was not originally PUT by this DB instance
        elif msg['primary'] != db_id:
            rating = None
            choices = []
            clocks = []
            for i in range(len(msg['clocks'])):
                key = msg['key']
                _, rating, choices, clocks = merge(key, None, msg['choices'][i], msg['clocks'][i])
                digest_list.append((msg['primary'], key, rating, choices, clocks))

    # At 'config['digest-length']'th write, fire everything in digest list to its neighbor
    if numWrites >= config['digest-length']:
        nextNeighbor = 'db'+str((id+1)%ndb)
        for digest in digest_list:
	    (primary, key, rating, choices, clocks) = digest
            queue.put(nextNeighbor, {'primary': primary, 'key': key, 'rating': rating, 'choices': choices, 'clocks': clocks})
        digest_list = []
        numWrites = 0

    # Return rating
    return {
            "rating": finalrating
    }

# Get the aggregate rating of entity
# This can be accesed as:
#   curl -XGET http://localhost:3000/rating/bob
# Response is a JSON object specifying the mean rating, choice list, and
# clock list for entity:
#   { rating: 5, choices: [5], clocks: [{c1: 3, c4: 10}] }
# This function also causes a gossip merge
@route('/rating/<entity>', method='GET')
def get_rating(entity):
    # YOUR CODE HERE
    # GOSSIP
    while 1:
        msg = queue.get(db_id)
        # Stop if there's no message in the queue
        if msg == None:
            break
        # Merge and pass on if the message was not originally PUT by this DB instance
        elif msg['primary'] != db_id:
            rating = None
            choices = []
            clocks = []
            for i in range(len(msg['clocks'])):
                key = msg['key']
                _, rating, choices, clocks = merge(key, None, msg['choices'][i], msg['clocks'][i])
                global digest_list
                digest_list.append((msg['primary'], key, rating, choices, clocks))

    key = '/rating/' + entity

    # GET THE VALUE FROM THE DATABASE
    # RETURN IT, REPLACING FOLLOWING
    rating = client.hget(key, 'rating')

    if rating == None:
        return {
            'rating': 0.0,
            'choices': [],
            'clocks': []
        }

    choices = eval(client.hget(key, 'choices'))
    clocks = eval(client.hget(key, 'clocks'))
    
    return {
        'rating': rating,
        'choices': choices,
        'clocks': clocks
    }

# Delete the rating information for entity
# This can be accessed as:
#   curl -XDELETE http://localhost:3000/rating/bob
# Response is a JSON object showing the new rating for the entity (always null)
#   { rating: null }
@route('/rating/<entity>', method='DELETE')
def delete_rating(entity):
    # ALREADY DONE--YOU DON'T NEED TO ADD ANYTHING
    count = client.delete('/rating/'+entity)
    if count == 0: return abort(404)
    return { "rating": None }

# Merge ratings and update DB
# PARAMS:
#    key       - key for the tea
#    oldrating - old rating (can be None, in which case the old rating will be retrieved
#                from the DB here)
#    setrating - new rating
#    setclock  - clock to be compared to vcl
# RETURN:
#    writeToDB   - whether a DB write occurs in this function
#    finalrating - the average rating for the tea
#    new_choices - the merged choices of ratings with corresponding clocks in new_vcl 
#    new_vcl     - the merged list of clocks
def merge(key, oldrating, setrating, setclock):
    finalrating = oldrating
    if oldrating == None:
        finalrating = client.hget(key, 'rating')
        # if a rating for the tea is not found in the DB, the rating is new. Add it.
        if finalrating == None:
            client.hset(key, 'rating', setrating)
            client.hset(key, 'choices', [setrating])
            client.hset(key, 'clocks', jsonify_vcl([setclock]))
            return True, setrating, [setrating], [setclock]
    choices = eval(client.hget(key, 'choices'))
    vcl = eval(client.hget(key, 'clocks'))
    new_vcl = []
    new_choices = []
    greaterThanAlreadyFound = False
    writeToDB = True
    for i in range(0, len(vcl)):
        old_clock = VectorClock.fromDict(vcl[i])
        # if the received clock is older, nothing needs updating
        if setclock <= old_clock:
            writeToDB = False
            break
        else:
            # if the received clock is newer, make changes accordingly
            if setclock > old_clock:
                # If we have not found an older clock and replaced it with the 
                # new one previously, put this new clock in. Otherwise, ignore.
                if not greaterThanAlreadyFound:
                    greaterThanAlreadyFound = True
                    new_vcl.append(setclock)
                    new_choices.append(setrating)
            # incomparable
            else:
                new_vcl.append(old_clock)
                new_choices.append(choices[i])

    # Update lists only if the received clock is not older than or the same as any of the
    # existing clocks. Otherwise, return empty choices.
    if writeToDB:
        # if the received clock is not newer than any of the existing clocks, it's
        # incomparable
        if not greaterThanAlreadyFound:
            new_vcl.append(setclock)
            new_choices.append(setrating)

        # calculate the new rating
        ratingSum = 0.0
        for choice in new_choices:
            ratingSum+=choice
        finalrating = ratingSum/len(new_choices)

        # update DB
        client.hset(key, 'rating', finalrating)
        client.hset(key, 'choices', new_choices)
        client.hset(key, 'clocks', jsonify_vcl(new_vcl))
    else:
        new_vcl = []
        new_choices = []

    return writeToDB, finalrating, new_choices, new_vcl

# Turn a list of vector clocks into a JSON formatted string to be stored in redis
def jsonify_vcl(vcl):
    i = 0
    json_str = '['
    for vc in vcl:
        json_str = json_str+'{'
        i += 1
        j = 0
        for key in vc.clock.keys():
            json_str = json_str+'"'+key+'"'+': '+str(vc.clock[key])
            j+=1
            if j < len(vc.clock.keys()):
                json_str = json_str+', '
        json_str = json_str+'}'
        if i < len(vcl):
            json_str = json_str + ', '
    json_str = json_str+']'
    return json_str

# Fire the engines
if __name__ == '__main__':
    run(host='0.0.0.0', port=os.getenv('PORT', config['hostport']), quiet=True)
