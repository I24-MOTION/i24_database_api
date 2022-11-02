#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 21 11:39:27 2022

@author: yanbing_wang
"""

from collections import OrderedDict, defaultdict
from multiprocessing.pool import ThreadPool
import numpy as np
import pandas as pd
import pymongo
from pymongo import UpdateOne

dt = 0.04
        
def thread_update_one(collection, filter, update, upsert=True):
    collection.update_one(filter, update, upsert)
    return

def thread_insert_one(collection, doc):
    collection.insert_one(doc)
    return
   

def decimal_range(start, stop, increment):
    while start < stop: # and not math.isclose(start, stop): Py>3.5
        yield start
        start += increment
        
        
        
def transform2(direction, config_params, chunk_size=None):
    '''
    direction: eb or wb
    query trajectories that starts in range [start_time, end_time)
    if they are specified. Otherwise from the b
    ** for static from_collection only **
    
    Similar to transform but of different schema
    temp schema:
    {
         {
            "_id":
            "timetamp": t1,
            "eb": {
                "traj1_id": [centerx, centery, l, w, dir, v],
                "traj2_id": [...],
                ...
                },
            "wb": {
                "traj3_id": [centerx, centery, l, w, dir, v],
                "traj4_id": [...],
                ...
                },
            },
        {
           "_id":
           "timetamp": t2,
           ...,
           ...
           },
    }   
    '''
    client_host=config_params['host']
    client_username=config_params['username']
    client_password=config_params['password']
    client_port=config_params['port']

    client=pymongo.MongoClient(host=client_host,
        port=client_port,
        username=client_username,
        password=client_password,
        connect=True,
        connectTimeoutMS=5000)

    # add schema to the meta collection
    schema_col = client[config_params["write_database_name"]]["__METADATA__"]
    first_doc = schema_col.find_one({})
    if not first_doc:
        schema_col.update_one({}, {"$set": {
            "schema."+config_params["write_collection_name"]:"xylwdv"}}, upsert=True)
    else:
        schema_col.update_one({"_id":first_doc["_id"]}, {"$set": {
            "schema."+config_params["write_collection_name"]:"xylwdv"}}, upsert=True)
    
    from_collection = client[config_params['read_database_name']][config_params['read_collection_name']]
    to_collection = client[config_params["write_database_name"]][config_params["write_collection_name"]]
    
    to_collection.create_index("timestamp")
    time_series_field = ["timestamp", "x_position", "y_position", "length", "width"]
        
    lru = OrderedDict()
    stale = defaultdict(int) # key: timestamp, val: number of timestamps that it hasn't been updated
    stale_thresh = 1000 # if a timestamp is not updated after processing [stale_thresh] number of trajs, then update to database. stale_thresh~=#veh on roadway simulataneously
    last_poped_t = 0
    
    dir = 1 if direction=="eb" else -1
    start = from_collection.find_one(sort=[("first_timestamp", 1)])["first_timestamp"]
    end = from_collection.find_one(sort=[("last_timestamp", -1)])["first_timestamp"]
    if not chunk_size:
        chunk_size = end-start +1 # query the entire collection
      
    # specify query - iterative ranges
    for s in decimal_range(start, end, chunk_size):
        print("Query range: {:.2f}-{:.2f}".format(s, s+chunk_size))
        all_trajs = from_collection.find({"direction":dir, "first_timestamp": {"$gte": s, "$lt": s+chunk_size}})
        
    # if not start_time and not end_time: # query the entire collection
    #     all_trajs = from_collection.find({"direction": dir})
    # elif start_time and end_time: # if time range is specified, query only the time range [start_time, end_time)
    #     all_trajs = from_collection.find({"direction":dir, "first_timestamp": {"$gte": start_time, "$lt": end_time}})
    # elif start_time:
    #     all_trajs = from_collection.find({"direction":dir, "first_timestamp": {"$gte": start_time}})
    # elif end_time:
    #     all_trajs = from_collection.find({"direction":dir, "first_timestamp": {"$lt": end_time}})
        
        bulk_write_cmd = []
        
        for traj in all_trajs:
            _id, l,w = traj["_id"], traj["length"], traj["width"]
            if isinstance(l, float):
                n = len(traj["x_position"])
                l,w = [l]*n, [w]*n # dumb but ok
                
            try:
                velocity = traj["velocity"]
            except KeyError:
                velocity = list(dir*np.diff(traj["x_position"])/dt)
                velocity.append(velocity[-1])
        
            # increment stale
            for k in stale:  
                stale[k] += 1
            
            
            # resample to 1/dt hz
            data = {key:traj[key] for key in time_series_field}
            data["velocity"] = velocity
            data["length"] = l
            data["width"] = w
            df = pd.DataFrame(data, columns=data.keys()) 
            index = pd.to_timedelta(df["timestamp"], unit='s')
            df = df.set_index(index)
            df = df.drop(columns = "timestamp")
            
            df=df.groupby(df.index.floor(str(dt)+"S")).mean().resample(str(dt)+"S").asfreq()
            df.index = df.index.values.astype('datetime64[ns]').astype('int64')*1e-9
            df = df.interpolate(method='linear')
            
            # assemble in traj
            # do not extrapolate for more than 1 sec
            first_valid_time = pd.Series.first_valid_index(df['x_position'])
            last_valid_time = pd.Series.last_valid_index(df['x_position'])
            first_time = max(min(traj['timestamp']), first_valid_time-1)
            last_time = min(max(traj['timestamp']), last_valid_time+1)
            df=df[first_time:last_time]
            
            # traj['x_position'] = list(df['x_position'].values)
            # traj['y_position'] = list(df['y_position'].values)
            # traj['timestamp'] = list(df.index.values)
            traj["first_timestamp"] = df.index.values[0]
            traj["last_timestamp"] = df.index.values[-1]
            traj["starting_x"] = df['x_position'].values[0]
            traj["ending_x"] = df['x_position'].values[-1]
    
            
            # add to result dictionary
            for t in df.index:
                # [centerx, centery, l ,w, dir, v]
                try:
                    lru[t][str(_id)] = [df["x_position"][t] + dir*0.5*df["length"][t],
                                        df["y_position"][t],
                                        df["length"][t],
                                        df["width"][t], 
                                        dir,
                                        df["velocity"][t]]
                except: # t does not exists in lru yet
                    if t <= last_poped_t:
                        # meaning t was poped pre-maturely
                        print("t was poped prematurely from LRU in transform_queue. Increase stale")
                        
                    lru[t] = {str(_id): [df["x_position"][t] + dir*0.5*df["length"][t],
                                        df["y_position"][t],
                                        df["length"][t],
                                        df["width"][t], 
                                        dir,
                                        df["velocity"][t]]}
                lru.move_to_end(t, last=True)
                stale[t] = 0 # reset staleness
                
            # update db from lru
            while stale[next(iter(lru))] > stale_thresh:
                t, d = lru.popitem(last=False) # pop first
                last_poped_t = t
                stale.pop(t)
                # change d to value.objectid: array, so that it does not reset the value field, but only update it
                query = {"timestamp": round(t,2)}
                update = {"$set": {direction+"."+key: val for key,val in d.items()}}
                bulk_write_cmd.append(UpdateOne(filter=query, update=update, upsert=True))
                
                # pool.apply_async(thread_update_one, (to_collection, {"timestamp": round(t,2)},{"$set": d},))
            
            # bulk write all the complete documents to db
            if len(bulk_write_cmd) > 500:
                to_collection.bulk_write(bulk_write_cmd, ordered=False)
                bulk_write_cmd = []
                
                
        # write the rest of lru to database
        print("Flush out the rest in LRU cache")
        while len(lru) > 0:
            t, d = lru.popitem(last=False) # pop first
            # d={direction+"."+key: val for key,val in d.items()}
            # pool.apply_async(thread_update_one, (to_collection, {"timestamp": round(t,2)},{"$set": d},))
            query = {"timestamp": round(t,2)}
            update = {"$set": {direction+"."+key: val for key,val in d.items()}}
            bulk_write_cmd.append(UpdateOne(filter=query, update=update, upsert=True))
            
        if len(bulk_write_cmd) > 0:
            to_collection.bulk_write(bulk_write_cmd, ordered=False)
            

    del to_collection
    del from_collection
    return 



if __name__ == '__main__':
    # copy a rec collection from one db to another
    
    # GET PARAMAETERS
    import json
    import os
    
    with open(os.environ["USER_CONFIG_DIRECTORY"]+"/db_param.json") as f:
        db_param = json.load(f)
        
    db_param["read_database_name"] = "trajectories"
    db_param["read_collection_name"] = "635997ddc8d071a13a9e5293"
    db_param["write_database_name"] = "transformed_beta"
    db_param["write_collection_name"] = db_param["read_collection_name"]
    
    transform2(chunk_size=20)
    
    
    
    
    
    
    
    
    