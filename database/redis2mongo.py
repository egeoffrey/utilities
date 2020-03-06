#!/usr/bin/python

## This script migrate eGeoffrey database from redis to mongodb

## CONFIGURATION
redis_hostname = "localhost"
redis_port = 6379
redis_database = 1

mongo_hostname = "localhost"
mongo_port = 27017
mongo_database = "egeoffrey"
mongo_username = "root"
mongo_password = "password"
#################

import sys
import redis
import pymongo
import linecache

version = "1.0"
mongo_database_schema = 1

# store database objects
db_redis = None
db_mongo_client = None
db_mongo = None

# Main
print "eGeoffrey Database Migration Utility v"+version
print ""
print "This script migrates eGeoffrey database from Redis to MongoDB."
print "Source: Redis database #"+str(redis_database)+" from "+redis_hostname+":"+str(redis_port)
print "Destination:  MongoDB database "+mongo_database+" on "+mongo_hostname+":"+str(mongo_port)
print "\nThe destination database will be deleted before starting the migration. Are you sure you want to migrate?"
raw_input("Press any key to continue...")

# connect to mongodb
try: 
    print "\nConnecting to mongo at "+mongo_hostname+":"+str(mongo_port)
    db_mongo_client = pymongo.MongoClient("mongodb://"+mongo_username+":"+mongo_password+"@"+mongo_hostname+":"+str(mongo_port)+"/"+str(mongo_database))
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
    
print "\nMigrating eGeoffrey database from redis to mongo..."
entries_count = 0
try:
    # delete all existing collections from mongodb
    collections = db_mongo.list_collection_names()
    for collection in collections:
        db_mongo[collection].drop()
    # retrieve all redis keys
    keys = db_redis.keys("*")
    keys.sort()
    for key in keys:
        # migrate database schema version
        if key == "eGeoffrey/version":
            document = {
                "value": str(mongo_database_schema)
            }
            db_mongo[key].insert_one(document)
            continue
        # create the collection 
        db_mongo.create_collection(key)
        # create the index
        db_mongo[key].create_index([("timestamp", pymongo.DESCENDING)])
        # migrate the entries
        entries = []
        # if a sorted set, ue zrange
        if db_redis.type(key) == "zset": 
            entries = db_redis.zrange(key, 0, -1)
        # else use get
        else:
            entries.append(db_redis.get(key))
        print "\tMigrating "+key+": "+str(len(entries))+" entries"
        entries_count = entries_count+len(entries)
        # for each item of the key
        for entry in entries:
            document = {}
            # if contains a timestamp
            if ":" in entry:
                split = entry.split(":", 1)
                timestamp = split[0]
                value = split[1]
                document = {
                    "timestamp": int(timestamp),
                    "value": str(value)
                }
            else: 
                document = {
                    "value": str(entry)
                }
            # insert into mongodb
            db_mongo[key].insert_one(document)
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
print "\nMigration completed of "+str(len(keys))+" keys for a total of "+str(entries_count)+" entries."
