#  Load balancer for Assignment 6, CMPT 474, Spring 2014

# Core libraries
import os
import sys
import time
import math
import json
import random
import string
import hashlib

# Libraries that have to have been installed by pip
import requests
import mimeparse
from bottle import route, run, request, response, abort

# Local libraries
from vectorclock import VectorClock

# These values are defaults for when you start this server from the command line
# They are overridden when you run it from test/run.py
config = {'id':0, 'port': 2500, 'db-base-port': 3000, 'ndb': 1 }

if (len(sys.argv) > 1):
    config = json.loads(sys.argv[1])

port = config['port']
ndb = config['ndb']
dbBasePort = config['db-base-port']

hash_algorithm = 'sha1'

# Update the rating of entity
# This can be accessed using;
#   curl -XPUT -H'Content-type: application/json' -d'{ "rating": 5, "clock": { "c1" : 5, "c2" : 3 } }' http://localhost:2500/rating/bob
# Response is a JSON object specifying the new mean rating for entity:
#   { rating: 5 }
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
    rating = data.get('rating')
    clock = VectorClock.fromDict(data.get('clock'))

    # Basic sanity checks on the rating
    if isinstance(rating, int): rating = float(rating)
    if not isinstance(rating, float): return abort(400)

    # YOUR CODE HERE
    # HASH THE ENTITY TO DETERMINE ITS SHARD
    # PUT THE PORT FOR THE CORRECT SHARD IN url below 
    url = 'http://localhost:'+str(dbBasePort+hashEntity(entity, ndb))+'/rating/'+entity

    # RESUME BOILERPLATE CODE...
    # Update the rating
    res = requests.put(url,
                       data=json.dumps({'rating': rating,
                                        'clock': clock.asDict()}),
                       headers={'content-type': 'application/json'})

    # Return the new rating for the entity
    return {
            "rating": res.json()['rating']
    }


# Get the aggregate rating of entity
# This can be accesed using:
#   curl -XGET http://localhost:2500/rating/bob
# By default, reads are strongly consistent. If you are willing
# tolerate an eventually consistent result from a random server (which may
# be faster, as this server might be less-loaded), add a 'consistency=weak'
# query:
#  curl -XGET http://localhost:2500/rating/bob?consistency=weak
# Response is a JSON object specifying the mean rating, choice list and
# clock list for entity:
#   { rating: 5, choices: [5], clocks: [{c1: 3, c4: 10}] }
@route('/rating/<entity>', method='GET')
def get_rating(entity):
    # YOUR CODE HERE
    # DETERMINE THE RIGHT DB INSTANCE TO CALL,
    # DEPENDING UPON WHETHER THE GET IS STRONGLY OR WEAKLY CONSISTENT
    # ASSIGN THE ENDPOINT TO url
    consistency = request.query.get('consistency')
    shard = 0
    # If weakly consistent, get the rating from a randomly selected DB.
    # Otherwise, hash the entity to get the primary instance.
    if consistency and consistency == 'weak':
        shard = random.randint(0, ndb-1)
    else:
	shard = hashEntity(entity, ndb)

    url = 'http://localhost:'+str(dbBasePort+shard)+'/rating/'+entity

    # RESUME BOILERPLATE
    curdata = requests.get(url).json()
    return {
            "rating":  curdata['rating'],
            "choices": curdata['choices'],
            "clocks":  curdata['clocks']
    }

# Delete the rating information for entity
# This can be accessed using:
#   curl -XDELETE http://localhost:2500/rating/bob
# Response is a JSON object showing the new rating for the entity (always null)
#   { rating: null }
@route('/rating/<entity>', method='DELETE')
def delete_rating(entity):
    # DONE---NOTHING TO CHANGE
    dbprimary = dbBasePort + hashEntity(entity, ndb)
    url = 'http://localhost:'+str(dbprimary)+'/rating/'+entity
    resp = requests.delete(url)
    return resp

# Determine the primary instance for the tea
def hashEntity(entity, numDBs):
	# Hash the entity to a hexadecimal value and then convert the value to int
	h = hashlib.new(hash_algorithm)
	h.update(entity)
	return int(long(h.hexdigest(), base=16) % numDBs)

# Fire the engines
if __name__ == '__main__':
    run(host='0.0.0.0', port=os.getenv('PORT', port), quiet=True)
