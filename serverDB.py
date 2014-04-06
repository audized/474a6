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
    finalrating = 0.0
    # SAVE NEW VALUES

    # GOSSIP

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
    key = '/rating/' + entity

    # YOUR CODE HERE
    # GOSSIP
    # GET THE VALUE FROM THE DATABASE
    # RETURN IT, REPLACING FOLLOWING
    
    return {
        'rating': 0.0,
        'choices': [],
        'clocks': []
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

# Fire the engines
if __name__ == '__main__':
    run(host='0.0.0.0', port=os.getenv('PORT', config['hostport']), quiet=True)
