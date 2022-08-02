#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 24 11:02:50 2022

@author: yanbing_wang
"""
from src.i24_database_api import DBClient
from i24_configparse import parse_cfg
import os
import json

# parameters path
if __name__ == '__main__':
    with open("config/db_param.json") as f:
        parameters = json.load(f)
    
    
    parameters["collection_name"] = "pristine_stork--RAW_GT1__excites"
    client = DBClient(**parameters)
    
    #%%
    # dbr.create_index(["ID", "first_timestamp", "last_timestamp", "starting_x", "ending_x"])
    print("collection name: ", client.collection_name)
    print("number of traj: ", client.count())
    
    print("min start time: ", client.get_min("first_timestamp"))
    print("max start time: ", client.get_max("first_timestamp"))
    
    print("min end time: ", client.get_min("last_timestamp"))
    print("max end time: ", client.get_max("last_timestamp"))
    
    print("min start x: ", client.get_min("starting_x"))
    print("max start x: ", client.get_max("starting_x"))
    
    print("min ending x: ", client.get_min("ending_x"))
    print("max ending x: ", client.get_max("ending_x"))
    
    # client.list_collection_names()
    
    #%%
    client.transform()
    
    
    
    
    
    
    
    
