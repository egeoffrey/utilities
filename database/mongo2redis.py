#!/usr/bin/python

## This script migrate eGeoffrey database from mongodb to redis
# usage:

## CONFIGURATION
mongo_hostname = "localhost"
mongo_port = 27017
mongo_database = "egeoffrey"
mongo_username = "root"
mongo_password = "password"

redis_hostname = "localhost"
redis_port = 6379
redis_database = 2
#################

import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import redis
import pymongo
import linecache

version = "1.0"
redis_database_schema = 1

# store database objects
db_redis = None
db_mongo_client = None
db_mongo = None

# Main
print "eGeoffrey Database Migration Utility v"+version
print ""
print "This script migrates eGeoffrey database from MongoDB to Redis."
print "Source:  MongoDB database "+mongo_database+" on "+mongo_hostname+":"+str(mongo_port)
print "Destination: Redis database #"+str(redis_database)+" from "+redis_hostname+":"+str(redis_port)
print "\nThe destination database will be deleted before starting the migration. Are you sure you want to migrate?"
raw_input("Press any key to continue...")

# connect to mongodb
try: 
    print "\nConnecting to mongo at "+mongo_hostname+":"+str(mongo_port)
    db_mongo_client = pymongo.MongoClient("mongodb://"+mongo_username+":"+mongo_password+"@"+mongo_hostname+":"+str(mongo_port)+"/")
    db_mongo = db_mongo_client[mongo_database]
    print "\tDatabase "+mongo_database+" has "+str(len(db_mongo.list_collection_names()))+" collections"
except Exception, e:
    print "Unable to connect to mongodb: "+str(e)
    sys.exit()

# connect to redis
try: 
    print "Connecting to redis at "+redis_hostname+":"+str(redis_port)
    db_redis = redis.StrictRedis(host=redis_hostname, port=redis_port, db=redis_database)
    print "\tDatabase "+str(redis_database)+" has "+str(len(db_redis.keys("*")))+" keys"
except Exception,e:
    print "Unable to connect to redis: "+str(e)
    sys.exit()
    
print "\nMigrating eGeoffrey database from mongodb to redis..."
entries_count = 0
try:
    # delete all existing collections from mongodb
    db_redis.flushdb()
    # retrieve all mongodb collections
    collections = db_mongo.list_collection_names()
    collections.sort()
    # for each collection
    for collection in collections:
        # migrate database schema version
        if collection == "eGeoffrey/version":
            db_redis.set(collection, str(redis_database_schema))
            continue
        documents = list(db_mongo[collection].find())
        # for each document
        print "\tMigrating "+collection+": "+str(len(documents))+" entries"
        for document in documents:
            # migrate timeseries data
            if "timestamp" in document:
                value = str(document["timestamp"])+":"+str(document["value"])
                db_redis.zadd(collection, document["timestamp"], value)
            # migrate simple values
            else:    
                db_redis.set(collection, str(document["value"]))
except Exception,e:
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)
    print "Error while migrating: line "+str(lineno)+" '"+line.strip()+"': "+str(exc_obj)
        
# close the connections
db_mongo_client.close()
db_redis.connection_pool.disconnect()
print "\nMigration completed of "+str(len(collections))+" keys for a total of "+str(entries_count)+" entries."