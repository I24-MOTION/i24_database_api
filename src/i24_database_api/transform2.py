#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 21 11:39:27 2022

@author: yanbing_wang
"""

from collections import OrderedDict, defaultdict
import numpy as np
import pandas as pd
import pymongo
from pymongo import UpdateOne
import queue
import time

dt = 0.04
class LRUCache:
    """
    A least-recently-used cache with integer capacity
    To roll out of the cache for vehicle color and dimensions
    get(): return the key-value in cache if exists, otherwise return -1
    put(): (no update) 
    """
    def __init__(self, capacity):
        self.cache = OrderedDict()
        self.capacity = capacity
 
    def get(self, key, default=None):
        
        try:
            item = self.cache[key]
            self.cache.move_to_end(key)
            return item
        except KeyError: # key not in cache
            self.cache[key] = default 
            return self.cache[key]
        
    def put(self, key, value):
        
        self.cache[key] = value
        
        if len(self.cache) > self.capacity:
            self.cache.popitem(last = False)
            
        self.cache.move_to_end(key)   
    

def decimal_range(start, stop, increment):
    while start < stop: # and not math.isclose(start, stop): Py>3.5
        yield start
        start += increment
        
        
        
def transform_beta(direction, config_params, bulk_write_que, chunk_size=50):
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
                "traj1_id": [centerx, centery, l, w, dir, v, a, cls, videonode],
                "traj2_id": [...],
                ...
                },
            "wb": {
                "traj3_id": [centerx, centery, l, w, dir, v, a, cls, videonode],
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
        
    schema in transformed_beta.__METADATA__
        {
            _id: RUN_ID,
            name: ""
            description: "",
            start_time: float,
            end_time: float,
            num_objects: int,
            duration: end_time-start_time,
            start_x:
            end_x:
            road_segment_length: 
        }
            
    '''
    # print("Chunk size: ", chunk_size)
    
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


    from_collection = client[config_params['read_database_name']][config_params['read_collection_name']]
    
    time_series_field = ["timestamp", "x_position", "y_position", "length", "width"]
      
    stale_thresh = 500 # if a timestamp is not updated after processing [stale_thresh] number of trajs, then update to database. stale_thresh~=#veh on roadway simulataneously
    # last_poped_t = 0
    
    dir = 1 if direction=="eb" else -1
    start = from_collection.find_one(sort=[("first_timestamp", 1)])["first_timestamp"]
    end = from_collection.find_one(sort=[("last_timestamp", -1)])["first_timestamp"]
    if not chunk_size:
        chunk_size = end-start +1 # query the entire collection
      
    # specify query - iterative ranges
    for s in decimal_range(start, end, chunk_size):
        
        print("{} In progress (approx) {:.1f} %".format(direction, (s-start)/(end-start)*100))
        
        lru = OrderedDict()
        attr_lru = LRUCache(1000)
        stale = defaultdict(int) # key: timestamp, val: number of timestamps that it hasn't been updated
        
        all_trajs = from_collection.find({"direction":dir, "first_timestamp": {"$gte": s, "$lt": s+chunk_size}}).sort("first_timestamp",1)
        
        for traj in all_trajs:
            
            _id, l,w,node, ccls = traj["_id"], traj["length"], traj["width"], traj["compute_node_id"], traj["coarse_vehicle_class"] 
            attr_lru.put(_id, [ccls,node])
            
            if isinstance(l, float):
                n = len(traj["x_position"])
                l,w = [l]*n, [w]*n # dumb but ok
                
            try:
                velocity = traj["velocity"]
            except KeyError:
                velocity = list(dir*np.diff(traj["x_position"])/dt)
                velocity.append(velocity[-1])
        
            # finite difference twice to get acceleration
            accel = list(np.diff(traj["x_position"], n=2)/(dt**2))
            last_accel = accel[-1]
            accel.extend([last_accel,last_accel]) 
            
            # increment stale
            for k in stale:
                stale[k] += 1
            
            # resample to 1/dt hz
            data = {key:traj[key] for key in time_series_field}
            data["velocity"] = velocity
            data["acceleration"] = accel
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

            traj["first_timestamp"] = df.index.values[0]
            traj["last_timestamp"] = df.index.values[-1]
            traj["starting_x"] = df['x_position'].values[0]
            traj["ending_x"] = df['x_position'].values[-1]
    
            
            # add to result dictionary
            for t in df.index:
                # [centerx, centery, l ,w, dir, v]
                ccls, node = attr_lru.get(_id)
                try:
                    
                    lru[t][str(_id)] = [df["x_position"][t] + dir*0.5*df["length"][t],
                                        df["y_position"][t],
                                        df["length"][t],
                                        df["width"][t], 
                                        dir,
                                        df["velocity"][t],
                                        df["acceleration"][t],
                                        ccls,
                                        node] # dir, v, a, cls, videonode],
                except: # t does not exists in lru yet
                    # if t <= last_poped_t:
                    #     # meaning t was poped pre-maturely
                    #     print("t was poped prematurely from LRU in transform_queue. Increase stale")
                        
                    lru[t] = {str(_id): [df["x_position"][t] + dir*0.5*df["length"][t],
                                        df["y_position"][t],
                                        df["length"][t],
                                        df["width"][t], 
                                        dir,
                                        df["velocity"][t],
                                        df["acceleration"][t],
                                        ccls,
                                        node] }
                lru.move_to_end(t, last=True)
                stale[t] = 0 # reset staleness
                
            # update db from lru
            while stale[next(iter(lru))] > stale_thresh:
                t, d = lru.popitem(last=False) # pop first
                # last_poped_t = t
                stale.pop(t)
                # change d to value.objectid: array, so that it does not reset the value field, but only update it
                query = {"timestamp": round(t,2)}
                update = {"$set": {direction+"."+key: val for key,val in d.items()}}
                bulk_write_que.put(UpdateOne(filter=query, update=update, upsert=True))
            
                
        # write the rest of lru to database
        # print("Flush out the rest in LRU cache")
        while len(lru) > 0:
            t, d = lru.popitem(last=False) # pop first
            # d={direction+"."+key: val for key,val in d.items()}
            # pool.apply_async(thread_update_one, (to_collection, {"timestamp": round(t,2)},{"$set": d},))
            query = {"timestamp": round(t,2)}
            update = {"$set": {direction+"."+key: val for key,val in d.items()}}
            bulk_write_que.put(UpdateOne(filter=query, update=update, upsert=True))
            

    del from_collection
    return 




def batch_write(config_params, bulk_write_queue, write_meta = False):
    
    time.sleep(10)
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


    from_collection = client[config_params['read_database_name']][config_params['read_collection_name']]
    to_collection = client[config_params["write_database_name"]][config_params["write_collection_name"]]
    to_collection.create_index("timestamp")
    
    # add schema to the meta collection
    if write_meta:
        meta_col = client[config_params["write_database_name"]]["__METADATA__"]
        start_time = from_collection.find_one(sort=[("first_timestamp", 1)])["first_timestamp"]
        end_time = from_collection.find_one(sort=[("last_timestamp", -1)])["last_timestamp"]
        start_x = from_collection.find_one(sort=[("starting_x", 1)])["starting_x"]
        end_x = from_collection.find_one(sort=[("ending_x", -1)])["ending_x"]
        
        meta_doc = {
            "_id": config_params['write_collection_name'],
            "name": "",
            "description": "",
            "start_time": start_time,
            "end_time": end_time,
            "num_objects": from_collection.estimated_document_count(),
            "duration": end_time-start_time,
            "start_x": start_x,
            "end_x": end_x, 
            "road_segment_length": abs(start_x-end_x)
            }
        try:
            meta_col.insert_one(meta_doc, bypass_document_validation=True)
        except:
            _id = config_params['write_collection_name']
            meta_doc.pop("_id")
            meta_col.update_one({"_id": _id}, {"$set": meta_doc}, upsert=True)
            
            
        print("Collection information is written to __METADATA__")

    bulk_write_cmd = []
    
    while True:
        try:
            cmd = bulk_write_queue.get(timeout = 5)
            bulk_write_cmd.append(cmd)
            
        except queue.Empty:
            print("Getting from bulk_write_queue reaches timeout.")
            break
        
        if len(bulk_write_cmd) > 500:
            to_collection.bulk_write(bulk_write_cmd, ordered=False)
            bulk_write_cmd = []
        
        
    if len(bulk_write_cmd) > 0:
        to_collection.bulk_write(bulk_write_cmd, ordered=False)
    
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
    
    transform_beta(chunk_size=20)
    
    # print("not implemented")
    
    
    
    
    
    
    
